//go:build integration

package testenv

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	tctidb "github.com/testcontainers/testcontainers-go/modules/tidb"
)

// A single TiDB serves the MySQL protocol on its own, with no PD or TiKV
// behind it. That is enough to be a rebuild target — the thing user SQL
// recomputes derived tables into — which is the only role TiDB plays in this
// tier. Change data capture is not part of it; see the package comment.
const tidbImage = "pingcap/tidb:v8.5.7"

// TiDB is a running database and an open connection to it.
type TiDB struct {
	DSN string
	DB  *sql.DB
}

// StartTiDB brings up a database for the duration of the test.
func StartTiDB(t *testing.T) *TiDB {
	t.Helper()
	ctx := context.Background()

	container, err := tctidb.Run(ctx, tidbImage)
	if err != nil {
		t.Fatalf("start tidb: %v\nis Docker running?", err)
	}
	t.Cleanup(func() { container.Terminate(context.Background()) })

	dsn, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("tidb connection string: %v", err)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open tidb: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.Ping(); err != nil {
		t.Fatalf("ping tidb at %s: %v", dsn, err)
	}

	return &TiDB{DSN: dsn, DB: db}
}
