// Command server runs the plata-ledger-core HTTP and gRPC APIs.
package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	"github.com/kamilch1k/plata-ledger-core/internal/api"
	"github.com/kamilch1k/plata-ledger-core/internal/grpcapi"
	"github.com/kamilch1k/plata-ledger-core/internal/ledger"
	pb "github.com/kamilch1k/plata-ledger-core/internal/ledgerpb"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL is required (e.g. postgres://user:pass@localhost:5432/ledger?sslmode=disable)")
	}
	port := envOr("PORT", "8080")
	grpcPort := envOr("GRPC_PORT", "9090")

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	store := ledger.New(pool)
	if err := store.Migrate(ctx); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	// gRPC surface over the same store.
	go func() {
		lis, err := net.Listen("tcp", ":"+grpcPort)
		if err != nil {
			log.Fatalf("grpc listen: %v", err)
		}
		gs := grpc.NewServer()
		pb.RegisterLedgerServer(gs, grpcapi.NewServer(store))
		log.Printf("plata-ledger-core gRPC listening on :%s", grpcPort)
		if err := gs.Serve(lis); err != nil {
			log.Fatalf("grpc serve: %v", err)
		}
	}()

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           api.NewServer(store),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("plata-ledger-core HTTP listening on :%s", port)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
