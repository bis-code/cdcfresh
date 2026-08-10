//go:build integration

package testenv

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// The same official image the local stack runs, pinned to one version.
	//
	// A single TiDB serves the MySQL protocol with no PD or TiKV behind it.
	// That is enough to be a rebuild target — the thing user SQL recomputes
	// derived tables into — which is the only role TiDB plays in this tier.
	// It emits no change data in this shape, and is not asked to; see the
	// package comment.
	tidbImage      = "pingcap/tidb:v8.5.7"
	tidbPort       = "4000/tcp"
	tidbStatusPort = "10080/tcp"
	tidbDSNFmt     = "root@tcp(%s:%s)/test?parseTime=true&loc=UTC"
)

// TiDB is a running database and an open connection to it.
type TiDB struct {
	DSN string
	DB  *sql.DB
}

// StartTiDB brings up a database for the duration of the test.
func StartTiDB(t *testing.T) *TiDB {
	t.Helper()
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        tidbImage,
		ExposedPorts: []string{tidbPort, tidbStatusPort},
		// The SQL port opens before bootstrap finishes, so a connection can be
		// accepted and then fail on the first statement. /status is the point
		// at which TiDB considers itself up.
		WaitingFor: wait.ForAll(
			wait.ForListeningPort(tidbPort),
			wait.ForHTTP("/status").WithPort(tidbStatusPort),
		),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start tidb: %v\nis Docker running?", err)
	}
	t.Cleanup(func() { container.Terminate(context.Background()) })

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("tidb host: %v", err)
	}
	port, err := container.MappedPort(ctx, "4000")
	if err != nil {
		t.Fatalf("tidb mapped port: %v", err)
	}
	dsn := fmt.Sprintf(tidbDSNFmt, host, port.Port())

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
