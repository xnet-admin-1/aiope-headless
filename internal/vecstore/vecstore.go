package vecstore

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"

	sqlite_vec "github.com/asg017/sqlite-vec-go-bindings/ncruces"
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
	CreatedAt int64  `json:"created_at"`
}

type SearchResult struct {
	ID       string  `json:"id"`
	Content  string  `json:"content"`
	Source   string  `json:"source"`
	Category string  `json:"category"`
	Distance float64 `json:"distance"`
}

func (v *VecStore) Init() error {
	_, err := v.DB.Exec(`CREATE VIRTUAL TABLE IF NOT EXISTS vec_docs USING vec0(
		doc_id TEXT PRIMARY KEY,
		embedding float[768] distance_metric=cosine,
		category TEXT partition_key,
		+content TEXT,
		+source TEXT,
		+created_at INTEGER
	)`)
	return err
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

func (v *VecStore) Add(doc Document) error {
	if doc.ID == "" {
		doc.ID = uuid.NewString()
	}
	if doc.CreatedAt == 0 {
		doc.CreatedAt = time.Now().Unix()
	}
	emb, err := v.Embed(doc.Content)
	if err != nil {
		return err
	}
	blob, err := sqlite_vec.SerializeFloat32(emb)
	if err != nil {
		return err
	}
	_, err = v.DB.Exec(`INSERT INTO vec_docs(doc_id, embedding, category, content, source, created_at) VALUES(?,?,?,?,?,?)`,
		doc.ID, blob, doc.Category, doc.Content, doc.Source, doc.CreatedAt)
	return err
}

func (v *VecStore) Search(query string, k int, category string) ([]SearchResult, error) {
	emb, err := v.Embed(query)
	if err != nil {
		return nil, err
	}
	blob, err := sqlite_vec.SerializeFloat32(emb)
	if err != nil {
		return nil, err
	}
	var rows *sql.Rows
	if category != "" {
		rows, err = v.DB.Query(`SELECT doc_id, distance, content, source, category FROM vec_docs WHERE embedding MATCH ? AND k = ? AND category = ?`, blob, k, category)
	} else {
		rows, err = v.DB.Query(`SELECT doc_id, distance, content, source, category FROM vec_docs WHERE embedding MATCH ? AND k = ?`, blob, k)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var results []SearchResult
	for rows.Next() {
		var r SearchResult
		if err := rows.Scan(&r.ID, &r.Distance, &r.Content, &r.Source, &r.Category); err != nil {
			return nil, err
		}
		results = append(results, r)
	}
	return results, nil
}

func (v *VecStore) Delete(id string) error {
	_, err := v.DB.Exec(`DELETE FROM vec_docs WHERE doc_id = ?`, id)
	return err
}

func (v *VecStore) List(category string, limit int) ([]Document, error) {
	if limit <= 0 {
		limit = 50
	}
	var rows *sql.Rows
	var err error
	if category != "" {
		rows, err = v.DB.Query(`SELECT doc_id, content, source, category, created_at FROM vec_docs WHERE category = ? LIMIT ?`, category, limit)
	} else {
		rows, err = v.DB.Query(`SELECT doc_id, content, source, category, created_at FROM vec_docs LIMIT ?`, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var docs []Document
	for rows.Next() {
		var d Document
		if err := rows.Scan(&d.ID, &d.Content, &d.Source, &d.Category, &d.CreatedAt); err != nil {
			return nil, err
		}
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
	stats := make(map[string]int)
	for rows.Next() {
		var cat string
		var count int
		if err := rows.Scan(&cat, &count); err != nil {
			return nil, err
		}
		stats[cat] = count
	}
	return stats, nil
}

func (v *VecStore) Reindex() error {
	rows, err := v.DB.Query(`SELECT doc_id, content FROM vec_docs`)
	if err != nil {
		return err
	}
	defer rows.Close()
	type item struct {
		id, content string
	}
	var items []item
	for rows.Next() {
		var it item
		if err := rows.Scan(&it.id, &it.content); err != nil {
			return err
		}
		items = append(items, it)
	}
	for _, it := range items {
		emb, err := v.Embed(it.content)
		if err != nil {
			return err
		}
		blob, err := sqlite_vec.SerializeFloat32(emb)
		if err != nil {
			return err
		}
		v.DB.Exec(`UPDATE vec_docs SET embedding = ? WHERE doc_id = ?`, blob, it.id)
	}
	return nil
}
