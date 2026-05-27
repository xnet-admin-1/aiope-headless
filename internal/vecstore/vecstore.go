package vecstore

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"time"

	"github.com/google/uuid"
)

type VecStore struct {
	DB       *sql.DB
	EmbedURL string
}

type Document struct {
	ID        string `json:"id"`
	Content   string `json:"content"`
	Source    string `json:"source"`
	Category  string `json:"category"`
	CreatedAt int64  `json:"createdAt"`
}

type SearchResult struct {
	ID       string  `json:"id"`
	Content  string  `json:"content"`
	Source   string  `json:"source"`
	Category string  `json:"category"`
	Distance float64 `json:"distance"`
}

func (v *VecStore) Init() error {
	_, err := v.DB.Exec(`CREATE TABLE IF NOT EXISTS vec_docs (
		id TEXT PRIMARY KEY,
		content TEXT NOT NULL,
		source TEXT NOT NULL DEFAULT '',
		category TEXT NOT NULL DEFAULT 'document',
		embedding BLOB,
		created_at INTEGER NOT NULL
	)`)
	if err != nil {
		return err
	}
	v.DB.Exec(`CREATE INDEX IF NOT EXISTS idx_vec_docs_cat ON vec_docs(category)`)
	return nil
}

func (v *VecStore) Embed(text string) ([]float32, error) {
	body, _ := json.Marshal(map[string]any{"input": text, "model": "jina"})
	resp, err := http.Post(v.EmbedURL+"/v1/embeddings", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var result struct {
		Data []struct {
			Embedding []float32 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	if len(result.Data) == 0 {
		return nil, fmt.Errorf("no embedding returned")
	}
	return result.Data[0].Embedding, nil
}

func serializeVec(v []float32) []byte {
	buf := make([]byte, len(v)*4)
	for i, f := range v {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(f))
	}
	return buf
}

func deserializeVec(b []byte) []float32 {
	v := make([]float32, len(b)/4)
	for i := range v {
		v[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))
	}
	return v
}

func cosine(a, b []float32) float64 {
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 1
	}
	return 1 - dot/(math.Sqrt(na)*math.Sqrt(nb))
}

func (v *VecStore) Add(doc Document) error {
	if doc.ID == "" {
		doc.ID = uuid.NewString()
	}
	if doc.CreatedAt == 0 {
		doc.CreatedAt = time.Now().UnixMilli()
	}
	emb, err := v.Embed(doc.Content)
	if err != nil {
		return err
	}
	_, err = v.DB.Exec(`INSERT OR REPLACE INTO vec_docs(id,content,source,category,embedding,created_at) VALUES(?,?,?,?,?,?)`,
		doc.ID, doc.Content, doc.Source, doc.Category, serializeVec(emb), doc.CreatedAt)
	return err
}

func (v *VecStore) Search(query string, k int, category string) ([]SearchResult, error) {
	qvec, err := v.Embed(query)
	if err != nil {
		return nil, err
	}
	q := `SELECT id, content, source, category, embedding FROM vec_docs`
	var args []any
	if category != "" {
		q += ` WHERE category = ?`
		args = append(args, category)
	}
	rows, err := v.DB.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []SearchResult
	for rows.Next() {
		var id, content, source, cat string
		var emb []byte
		rows.Scan(&id, &content, &source, &cat, &emb)
		if emb == nil {
			continue
		}
		dist := cosine(qvec, deserializeVec(emb))
		results = append(results, SearchResult{ID: id, Content: content, Source: source, Category: cat, Distance: dist})
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Distance < results[j].Distance })
	if k > 0 && len(results) > k {
		results = results[:k]
	}
	return results, nil
}

func (v *VecStore) Delete(id string) error {
	_, err := v.DB.Exec(`DELETE FROM vec_docs WHERE id = ?`, id)
	return err
}

func (v *VecStore) List(category string, limit int) ([]Document, error) {
	q := `SELECT id, content, source, category, created_at FROM vec_docs`
	var args []any
	if category != "" {
		q += ` WHERE category = ?`
		args = append(args, category)
	}
	q += ` ORDER BY created_at DESC LIMIT ?`
	args = append(args, limit)
	rows, err := v.DB.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var docs []Document
	for rows.Next() {
		var d Document
		rows.Scan(&d.ID, &d.Content, &d.Source, &d.Category, &d.CreatedAt)
		docs = append(docs, d)
	}
	return docs, nil
}

func (v *VecStore) Stats() (map[string]int, error) {
	rows, err := v.DB.Query(`SELECT category, COUNT(*) FROM vec_docs GROUP BY category`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	m := map[string]int{}
	for rows.Next() {
		var cat string
		var count int
		rows.Scan(&cat, &count)
		m[cat] = count
	}
	return m, nil
}

func (v *VecStore) Reindex() error {
	rows, err := v.DB.Query(`SELECT id, content FROM vec_docs`)
	if err != nil {
		return err
	}
	defer rows.Close()
	type item struct{ id, content string }
	var items []item
	for rows.Next() {
		var it item
		rows.Scan(&it.id, &it.content)
		items = append(items, it)
	}
	for _, it := range items {
		emb, err := v.Embed(it.content)
		if err != nil {
			continue
		}
		v.DB.Exec(`UPDATE vec_docs SET embedding = ? WHERE id = ?`, serializeVec(emb), it.id)
	}
	return nil
}
