// Command server runs the plata-ledger-core HTTP and gRPC APIs, drains the
// event outbox to the message bus, and (when Kafka is configured) runs the AML
// consumer.
package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	"github.com/kamilch1k/plata-ledger-core/internal/aml"
	"github.com/kamilch1k/plata-ledger-core/internal/api"
	"github.com/kamilch1k/plata-ledger-core/internal/events"
	"github.com/kamilch1k/plata-ledger-core/internal/grpcapi"
	"github.com/kamilch1k/plata-ledger-core/internal/kafka"
	"github.com/kamilch1k/plata-ledger-core/internal/ledger"
	pb "github.com/kamilch1k/plata-ledger-core/internal/ledgerpb"
	"github.com/kamilch1k/plata-ledger-core/internal/origination"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL is required (e.g. postgres://user:pass@localhost:5432/ledger?sslmode=disable)")
	}
	port := envOr("PORT", "8080")
	grpcPort := envOr("GRPC_PORT", "9090")
	topic := envOr("KAFKA_TOPIC", "ledger.events")
	var brokers []string
	if b := os.Getenv("KAFKA_BROKERS"); b != "" {
		brokers = strings.Split(b, ",")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	store := ledger.New(pool)
	origStore := origination.New(pool, store)
	amlConsumer := aml.New(pool)
	for name, mig := range map[string]func(context.Context) error{
		"ledger":      store.Migrate,
		"events":      func(c context.Context) error { return events.Migrate(c, pool) },
		"origination": origStore.Migrate,
		"aml":         amlConsumer.Migrate,
	} {
		if err := mig(ctx); err != nil {
			log.Fatalf("migrate %s: %v", name, err)
		}
	}

	// Outbox publisher: real Kafka when configured, otherwise a logging stub so
	// the relay still drains locally.
	var publisher events.Publisher = logPublisher{}
	if len(brokers) > 0 {
		kp := kafka.NewPublisher(brokers, topic)
		defer func() { _ = kp.Close() }()
		publisher = kp
		startAMLConsumer(ctx, brokers, topic, amlConsumer)
	}

	// Relay: drain the outbox every few seconds.
	relay := events.NewRelay(events.NewStore(pool), publisher, 100)
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for range t.C {
			if _, err := relay.PublishPending(ctx); err != nil {
				log.Printf("relay: %v", err)
			}
		}
	}()

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
		Handler:           api.NewServer(store, origStore),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("plata-ledger-core HTTP listening on :%s", port)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func startAMLConsumer(ctx context.Context, brokers []string, topic string, c *aml.Consumer) {
	cons := kafka.NewConsumer(brokers, topic, "aml")
	go func() {
		err := cons.Run(ctx, func(ctx context.Context, e events.Event) error {
			_, perr := c.Process(ctx, []events.Event{e})
			return perr
		})
		log.Printf("aml consumer stopped: %v", err)
	}()
}

// logPublisher is the default outbox sink when Kafka is not configured.
type logPublisher struct{}

func (logPublisher) Publish(_ context.Context, evs []events.Event) error {
	for _, e := range evs {
		log.Printf("[event] %s aggregate=%s", e.Type, e.AggregateID)
	}
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
