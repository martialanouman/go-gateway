// Command fake-smsc runs the in-repo fake SMSC as a standalone process (plan §1.8): a minimal SMPP
// peer that accepts a bind and answers submit_sm with OK. It backs `make fake-smsc` for driving the
// pipeline locally before the real simulator (M8). It is a development tool, not a deployable
// service.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/testutil/fakesmsc"
)

func main() {
	addr := flag.String("addr", ":2775", "SMPP listen address")
	flag.Parse()

	srv, err := fakesmsc.New(fakesmsc.Config{
		Addr:     *addr,
		OnSubmit: func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() },
	})
	if err != nil {
		log.Fatalf("fake-smsc: %v", err)
	}
	defer srv.Close()

	log.Printf("fake SMSC listening on %s (submit_sm -> OK)", srv.Addr())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	<-ctx.Done()
	log.Print("fake SMSC shutting down")
}
