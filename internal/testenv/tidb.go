//go:build integration

package testenv

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

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
)

var (
	tidbOnce sync.Once
	tidbInst *TiDB
	tidbErr  error
)

// TiDB is a running database. Use Schema for a handle scoped to one test.
type TiDB struct {
	DSN  string
	DB   *sql.DB
	addr string // host:port, for building per-schema DSNs
}

// SharedTiDB returns a database started once for this test binary. See
// SharedPulsar for why it registers no cleanup.
func SharedTiDB(t *testing.T) *TiDB {
	t.Helper()
	tidbOnce.Do(func() { tidbInst, tidbErr = startTiDB(context.Background()) })
	if tidbErr != nil {
		t.Fatalf("start tidb: %v\nis Docker running?", tidbErr)
	}
	return tidbInst
}

// Schema creates a database unique to the calling test and returns a handle
// scoped to it.
//
// The DROP on cleanup is hygiene, not isolation: the unique name is what keeps
// tests apart, so a missed drop leaves rubbish behind but cannot affect
// another test.
func (d *TiDB) Schema(t *testing.T) *sql.DB {
	t.Helper()
	name := fmt.Sprintf("s_%s_%d", safeName(t.Name()), time.Now().UnixNano())
	if _, err := d.DB.Exec("CREATE DATABASE " + name); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	t.Cleanup(func() { d.DB.Exec("DROP DATABASE IF EXISTS " + name) })

	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(%s)/%s?parseTime=true&loc=UTC", d.addr, name))
	if err != nil {
		t.Fatalf("open %s: %v", name, err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.Ping(); err != nil {
		t.Fatalf("ping %s: %v", name, err)
	}
	return db
}

func startTiDB(ctx context.Context) (*TiDB, error) {
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
		return nil, fmt.Errorf("start container: %w", err)
	}
	host, err := container.Host(ctx)
	if err != nil {
		return nil, fmt.Errorf("host: %w", err)
	}
	port, err := container.MappedPort(ctx, "4000")
	if err != nil {
		return nil, fmt.Errorf("mapped port: %w", err)
	}
	addr := fmt.Sprintf("%s:%s", host, port.Port())
	dsn := fmt.Sprintf("root@tcp(%s)/test?parseTime=true&loc=UTC", addr)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping %s: %w", dsn, err)
	}
	return &TiDB{DSN: dsn, DB: db, addr: addr}, nil
}
