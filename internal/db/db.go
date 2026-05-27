package db

import (
	_ "embed"

	"database/sql"

	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
)

//go:embed schema.sql
var schema string

func Open(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.Exec("PRAGMA journal_mode=WAL")
	db.Exec("PRAGMA foreign_keys=ON")
	db.Exec("PRAGMA busy_timeout=5000")
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, err
	}
	db.Exec(`INSERT OR IGNORE INTO settings_kv(key,value) VALUES('agent_tools_tool_guidance','You have access to tools. Use them whenever you need to search, read files, run commands, or perform actions. Do NOT describe what you would do — actually call the tools. Never fabricate tool output.')`)
	db.Exec(`INSERT OR IGNORE INTO settings_kv(key,value) VALUES('search_provider_url','https://search.xnet.ngo')`)
	// Migrations
	db.Exec(`ALTER TABLE messages ADD COLUMN reasoning TEXT NOT NULL DEFAULT ''`)
	return db, nil
}
