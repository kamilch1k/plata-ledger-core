package ledger

import (
	"context"
	"fmt"
	"testing"
)

// BenchmarkTransfer measures sequential transfer throughput against real
// Postgres. Run: go test -bench=BenchmarkTransfer ./internal/ledger
func BenchmarkTransfer(b *testing.B) {
	ctx := context.Background()
	if _, err := testPool.Exec(ctx, `TRUNCATE ledger_entries, transfers, accounts RESTART IDENTITY CASCADE`); err != nil {
		b.Fatal(err)
	}
	s := New(testPool)
	from, err := s.CreateAccount(ctx, "From", "USD", int64(b.N)+1_000)
	if err != nil {
		b.Fatal(err)
	}
	to, err := s.CreateAccount(ctx, "To", "USD", 0)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := s.Transfer(ctx, TransferParams{
			IdempotencyKey: fmt.Sprintf("bench-%d", i),
			FromAccount:    from.ID,
			ToAccount:      to.ID,
			AmountMinor:    1,
		}); err != nil {
			b.Fatal(err)
		}
	}
}
