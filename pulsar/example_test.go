package pulsar_test

import (
	"context"
	"database/sql"
	"log"
	"time"

	"github.com/bis-code/cdcfresh"
	"github.com/bis-code/cdcfresh/pulsar"
)

// Example wires a TiCDC changefeed on Pulsar to a derived table.
//
// Note what the rebuild does not do: it never reads the event. The change
// only says which device went stale; the total is recomputed from the source
// table, so a duplicate delivery is harmless and drift is impossible.
func Example() {
	var db *sql.DB // your database handle

	src, err := pulsar.Source(
		"pulsar://localhost:6650",
		[]string{"persistent://public/default/cdcfresh"},
		pulsar.WithSubscription("device-totals"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer src.Close()

	r, err := cdcfresh.New(
		cdcfresh.Source(src),

		cdcfresh.Scope(func(ev cdcfresh.RowEvent) []cdcfresh.Key {
			if ev.Table != "readings" {
				return nil
			}
			row := ev.Data
			if row == nil {
				row = ev.Old // a delete carries the previous image
			}
			device, _ := row["device"].(string)
			if device == "" {
				return nil
			}
			return []cdcfresh.Key{cdcfresh.Key(device)}
		}),

		cdcfresh.Rebuild(func(ctx context.Context, k cdcfresh.Key) error {
			_, err := db.ExecContext(ctx,
				`REPLACE INTO device_totals (device, total)
				 SELECT device, SUM(value) FROM readings WHERE device = ? GROUP BY device`,
				string(k))
			return err
		}),

		cdcfresh.Coalesce(5*time.Second),
		cdcfresh.MaxWait(30*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}

	if err := r.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
