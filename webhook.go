package maxigobot

import (
	gocontext "context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime/debug"
	"time"
)

const (
	defaultWebhookEndpoint    = "/"
	defaultReadHeaderTimeout  = 5 * time.Second
	defaultShutdownTimeout    = 10 * time.Second
	webhookMaxBodySize        = 1 << 20 // 1 MB
)

// WebhookPoller implements Poller by receiving updates via HTTP webhook.
//
// The MAX API sends a POST request for each update to the registered webhook URL.
// Each request body is a single JSON update object.
//
// Use [Client.Subscribe] to register the webhook URL before starting the bot.
type WebhookPoller struct {
	// Listen is the address to bind the HTTP server to (e.g. ":8080", "0.0.0.0:443").
	// Required.
	Listen string

	// Endpoint is the HTTP path to accept updates on (default: "/").
	Endpoint string

	// Server allows injecting a pre-configured *http.Server.
	// If nil, a default server is created with Listen as Addr.
	// When set, Listen is ignored (Addr from Server is used).
	Server *http.Server
}

// Poll starts an HTTP server and feeds incoming webhook updates into the channel.
// It blocks until stop is closed, then gracefully shuts down the server.
// The updates channel is closed before returning.
func (w *WebhookPoller) Poll(b *Bot, updates chan<- any, stop chan struct{}) {
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("panic in webhook poller: %v\n%s", r, debug.Stack())
			b.handleError(err, nil, "webhook_poller")
		}
	}()
	defer close(updates)

	endpoint := w.Endpoint
	if endpoint == "" {
		endpoint = defaultWebhookEndpoint
	}

	mux := http.NewServeMux()
	mux.HandleFunc(endpoint, func(rw http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(rw, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(io.LimitReader(r.Body, webhookMaxBodySize))
		if err != nil {
			b.handleError(fmt.Errorf("webhook: read body: %w", err), nil, "webhook_poller")
			http.Error(rw, "bad request", http.StatusBadRequest)
			return
		}

		upd, err := ParseUpdate(json.RawMessage(body))
		if err != nil {
			b.handleError(fmt.Errorf("webhook: parse update: %w", err), nil, "webhook_poller")
			http.Error(rw, "bad request", http.StatusBadRequest)
			return
		}
		if upd == nil {
			// Unknown update type — acknowledge silently.
			rw.WriteHeader(http.StatusOK)
			return
		}

		updates <- upd
		rw.WriteHeader(http.StatusOK)
	})

	srv := w.Server
	if srv == nil {
		srv = &http.Server{
			Addr:              w.Listen,
			ReadHeaderTimeout: defaultReadHeaderTimeout,
		}
	}
	srv.Handler = mux

	// Start server in a goroutine.
	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	// Wait for stop signal or server error.
	select {
	case <-stop:
	case err := <-errCh:
		if err != nil {
			b.handleError(fmt.Errorf("webhook: server error: %w", err), nil, "webhook_poller")
		}
		return
	}

	// Graceful shutdown.
	ctx, cancel := gocontext.WithTimeout(gocontext.Background(), defaultShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		b.handleError(fmt.Errorf("webhook: shutdown error: %w", err), nil, "webhook_poller")
	}
}