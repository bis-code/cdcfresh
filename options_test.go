package cdcfresh

import (
	"context"
	"strings"
	"testing"
	"time"
)

type stubSource struct{}

func (stubSource) Receive(ctx context.Context) (Event, error) {
	<-ctx.Done()
	return Event{}, ctx.Err()
}

func requiredOpts() []Option {
	return []Option{
		Source(stubSource{}),
		Scope(func(RowEvent) []Key { return nil }),
		Rebuild(func(context.Context, Key) error { return nil }),
	}
}

func TestNewMissingRequiredOptions(t *testing.T) {
	_, err := New()
	if err == nil {
		t.Fatal("New() with no options: want error, got nil")
	}
	for _, want := range []string{"Source", "Scope", "Rebuild"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name missing option %q", err, want)
		}
	}
}

func TestNewMissingOneOption(t *testing.T) {
	_, err := New(requiredOpts()[:2]...) // Source + Scope, no Rebuild
	if err == nil || !strings.Contains(err.Error(), "Rebuild") {
		t.Fatalf("want error naming Rebuild, got %v", err)
	}
	if err != nil && strings.Contains(err.Error(), "Source") {
		t.Errorf("error %q names Source, which was provided", err)
	}
}

func TestNewDefaults(t *testing.T) {
	r, err := New(requiredOpts()...)
	if err != nil {
		t.Fatal(err)
	}
	c := r.cfg
	if c.coalesce != 5*time.Second || c.maxWait != 30*time.Second ||
		c.workers != 4 || c.backoffBase != time.Second ||
		c.backoffCap != 5*time.Minute || c.poisonAfter != 10 {
		t.Errorf("defaults wrong: %+v", c)
	}
	if c.onError == nil {
		t.Error("onError default must be non-nil no-op")
	}
}

func TestNewRejectsInvalidValues(t *testing.T) {
	cases := []struct {
		name string
		opt  Option
		want string
	}{
		{"Workers(0)", Workers(0), "Workers"},
		{"Coalesce(-1)", Coalesce(-1), "Coalesce"},
		{"PoisonAfter(0)", PoisonAfter(0), "PoisonAfter"},
		{"Reconcile(0, fn)", Reconcile(0, func(context.Context) ([]Key, error) { return nil, nil }), "Reconcile"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(append(requiredOpts(), tc.opt)...)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error naming %q, got %v", tc.want, err)
			}
		})
	}
}

func TestNewValidConfigStillConstructs(t *testing.T) {
	_, err := New(append(requiredOpts(),
		Workers(1), Coalesce(time.Millisecond), PoisonAfter(1),
		Reconcile(time.Second, func(context.Context) ([]Key, error) { return nil, nil }),
	)...)
	if err != nil {
		t.Fatalf("valid config: unexpected error %v", err)
	}
}

func TestOptionsApply(t *testing.T) {
	r, err := New(append(requiredOpts(),
		Coalesce(time.Second), MaxWait(2*time.Second), Workers(1),
		Backoff(10*time.Millisecond, time.Second), PoisonAfter(3),
	)...)
	if err != nil {
		t.Fatal(err)
	}
	c := r.cfg
	if c.coalesce != time.Second || c.maxWait != 2*time.Second || c.workers != 1 ||
		c.backoffBase != 10*time.Millisecond || c.backoffCap != time.Second || c.poisonAfter != 3 {
		t.Errorf("options not applied: %+v", c)
	}
}
