package maxigobot

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	maxigo "github.com/maxigo-bot/maxigo-client"
)

// freePort asks the OS for an available port.
func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

// webhookTestBot creates a minimal Bot suitable for webhook tests.
func webhookTestBot(t *testing.T) *Bot {
	t.Helper()
	b, err := New("test-token")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return b
}

func TestWebhookPoller_ReceivesUpdate(t *testing.T) {
	addr := freePort(t)
	b := webhookTestBot(t)

	updates := make(chan any, 10)
	stop := make(chan struct{})

	poller := &WebhookPoller{Listen: addr}
	go poller.Poll(b, updates, stop)

	// Give the server a moment to start.
	time.Sleep(50 * time.Millisecond)

	body := `{
		"update_type": "message_created",
		"timestamp": 1000,
		"message": {
			"sender": {"user_id": 1, "first_name": "Test", "is_bot": false, "last_activity_time": 0},
			"recipient": {"chat_id": 2, "chat_type": "dialog"},
			"timestamp": 1000,
			"body": {"mid": "m1", "seq": 1, "text": "hello"}
		}
	}`

	resp, err := http.Post("http://"+addr+"/", "application/json", strings.NewReader(body))
	if err != nil {
		close(stop)
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		close(stop)
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	select {
	case upd := <-updates:
		msg, ok := upd.(*maxigo.MessageCreatedUpdate)
		if !ok {
			t.Fatalf("expected *maxigo.MessageCreatedUpdate, got %T", upd)
		}
		if msg.Message.Body.MID != "m1" {
			t.Errorf("MID = %q, want %q", msg.Message.Body.MID, "m1")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for update")
	}

	close(stop)
	// Drain to let Poll finish.
	for range updates {
	}
}

func TestWebhookPoller_AllUpdateTypes(t *testing.T) {
	addr := freePort(t)
	b := webhookTestBot(t)

	updates := make(chan any, 20)
	stop := make(chan struct{})

	poller := &WebhookPoller{Listen: addr}
	go poller.Poll(b, updates, stop)
	time.Sleep(50 * time.Millisecond)

	types := []struct {
		name string
		body string
	}{
		{"message_created", `{"update_type":"message_created","timestamp":1,"message":{"sender":{"user_id":1,"first_name":"T","is_bot":false,"last_activity_time":0},"recipient":{"chat_id":2,"chat_type":"dialog"},"timestamp":1,"body":{"mid":"m1","seq":1,"text":"hi"}}}`},
		{"message_callback", `{"update_type":"message_callback","timestamp":1,"callback":{"timestamp":1,"callback_id":"c1","payload":"p","user":{"user_id":1,"first_name":"T","is_bot":false,"last_activity_time":0}},"message":{"sender":{"user_id":1,"first_name":"T","is_bot":false,"last_activity_time":0},"recipient":{"chat_id":2,"chat_type":"dialog"},"timestamp":1,"body":{"mid":"m1","seq":1}}}`},
		{"message_edited", `{"update_type":"message_edited","timestamp":1,"message":{"sender":{"user_id":1,"first_name":"T","is_bot":false,"last_activity_time":0},"recipient":{"chat_id":2,"chat_type":"dialog"},"timestamp":1,"body":{"mid":"m1","seq":1,"text":"edited"}}}`},
		{"message_removed", `{"update_type":"message_removed","timestamp":1,"message_id":"m1","chat_id":2,"user_id":1}`},
		{"bot_started", `{"update_type":"bot_started","timestamp":1,"chat_id":123,"user":{"user_id":1,"first_name":"T","is_bot":false,"last_activity_time":0}}`},
		{"bot_stopped", `{"update_type":"bot_stopped","timestamp":1,"chat_id":123,"user":{"user_id":1,"first_name":"T","is_bot":false,"last_activity_time":0}}`},
		{"bot_added", `{"update_type":"bot_added","timestamp":1,"chat_id":123,"user":{"user_id":1,"first_name":"T","is_bot":false,"last_activity_time":0}}`},
		{"bot_removed", `{"update_type":"bot_removed","timestamp":1,"chat_id":123,"user":{"user_id":1,"first_name":"T","is_bot":false,"last_activity_time":0}}`},
		{"user_added", `{"update_type":"user_added","timestamp":1,"chat_id":123,"user":{"user_id":1,"first_name":"T","is_bot":false,"last_activity_time":0},"inviter_id":2}`},
		{"user_removed", `{"update_type":"user_removed","timestamp":1,"chat_id":123,"user":{"user_id":1,"first_name":"T","is_bot":false,"last_activity_time":0},"admin_id":2}`},
		{"chat_title_changed", `{"update_type":"chat_title_changed","timestamp":1,"chat_id":123,"user":{"user_id":1,"first_name":"T","is_bot":false,"last_activity_time":0},"title":"New"}`},
		{"message_chat_created", `{"update_type":"message_chat_created","timestamp":1,"chat":{"chat_id":123,"type":"dialog","status":"active","title":"T","participants_count":2,"is_public":false},"message_id":"m1","start_payload":"p"}`},
		{"dialog_muted", `{"update_type":"dialog_muted","timestamp":1,"user":{"user_id":1,"first_name":"T","is_bot":false,"last_activity_time":0}}`},
		{"dialog_unmuted", `{"update_type":"dialog_unmuted","timestamp":1,"user":{"user_id":1,"first_name":"T","is_bot":false,"last_activity_time":0}}`},
		{"dialog_cleared", `{"update_type":"dialog_cleared","timestamp":1,"user":{"user_id":1,"first_name":"T","is_bot":false,"last_activity_time":0}}`},
		{"dialog_removed", `{"update_type":"dialog_removed","timestamp":1,"user":{"user_id":1,"first_name":"T","is_bot":false,"last_activity_time":0}}`},
	}

	for _, tc := range types {
		resp, err := http.Post("http://"+addr+"/", "application/json", strings.NewReader(tc.body))
		if err != nil {
			close(stop)
			t.Fatalf("%s: POST error: %v", tc.name, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			close(stop)
			t.Fatalf("%s: status = %d, want 200", tc.name, resp.StatusCode)
		}
	}

	received := 0
	timeout := time.After(3 * time.Second)
	for received < len(types) {
		select {
		case <-updates:
			received++
		case <-timeout:
			close(stop)
			t.Fatalf("timed out: received %d/%d updates", received, len(types))
		}
	}

	close(stop)
	for range updates {
	}
}

func TestWebhookPoller_CustomEndpoint(t *testing.T) {
	addr := freePort(t)
	b := webhookTestBot(t)

	updates := make(chan any, 10)
	stop := make(chan struct{})

	poller := &WebhookPoller{Listen: addr, Endpoint: "/webhook"}
	go poller.Poll(b, updates, stop)
	time.Sleep(50 * time.Millisecond)

	body := `{"update_type":"bot_started","timestamp":1,"chat_id":1,"user":{"user_id":1,"first_name":"T","is_bot":false,"last_activity_time":0}}`

	// Wrong endpoint should 404.
	resp, err := http.Post("http://"+addr+"/wrong", "application/json", strings.NewReader(body))
	if err != nil {
		close(stop)
		t.Fatalf("POST /wrong: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("/wrong: status = %d, want 404", resp.StatusCode)
	}

	// Correct endpoint should work.
	resp, err = http.Post("http://"+addr+"/webhook", "application/json", strings.NewReader(body))
	if err != nil {
		close(stop)
		t.Fatalf("POST /webhook: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/webhook: status = %d, want 200", resp.StatusCode)
	}

	select {
	case upd := <-updates:
		if _, ok := upd.(*maxigo.BotStartedUpdate); !ok {
			t.Errorf("expected *maxigo.BotStartedUpdate, got %T", upd)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for update")
	}

	close(stop)
	for range updates {
	}
}

func TestWebhookPoller_MethodNotAllowed(t *testing.T) {
	addr := freePort(t)
	b := webhookTestBot(t)

	updates := make(chan any, 10)
	stop := make(chan struct{})

	poller := &WebhookPoller{Listen: addr}
	go poller.Poll(b, updates, stop)
	time.Sleep(50 * time.Millisecond)

	resp, err := http.Get("http://" + addr + "/")
	if err != nil {
		close(stop)
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", resp.StatusCode)
	}

	close(stop)
	for range updates {
	}
}

func TestWebhookPoller_InvalidJSON(t *testing.T) {
	addr := freePort(t)
	b := webhookTestBot(t)

	var mu sync.Mutex
	var gotErr error
	b.OnError = func(err error, c Context) {
		mu.Lock()
		defer mu.Unlock()
		if gotErr == nil {
			gotErr = err
		}
	}

	updates := make(chan any, 10)
	stop := make(chan struct{})

	poller := &WebhookPoller{Listen: addr}
	go poller.Poll(b, updates, stop)
	time.Sleep(50 * time.Millisecond)

	resp, err := http.Post("http://"+addr+"/", "application/json", strings.NewReader(`{invalid`))
	if err != nil {
		close(stop)
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}

	mu.Lock()
	defer mu.Unlock()
	if gotErr == nil || !strings.Contains(gotErr.Error(), "webhook: parse update") {
		t.Errorf("expected 'webhook: parse update' error, got: %v", gotErr)
	}

	close(stop)
	for range updates {
	}
}

func TestWebhookPoller_UnknownUpdateType(t *testing.T) {
	addr := freePort(t)
	b := webhookTestBot(t)

	updates := make(chan any, 10)
	stop := make(chan struct{})

	poller := &WebhookPoller{Listen: addr}
	go poller.Poll(b, updates, stop)
	time.Sleep(50 * time.Millisecond)

	resp, err := http.Post("http://"+addr+"/", "application/json",
		strings.NewReader(`{"update_type":"future_event","timestamp":1}`))
	if err != nil {
		close(stop)
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200 for unknown type", resp.StatusCode)
	}

	// Nothing should appear in updates.
	select {
	case upd := <-updates:
		t.Errorf("unexpected update: %v", upd)
	case <-time.After(100 * time.Millisecond):
		// Good — no update.
	}

	close(stop)
	for range updates {
	}
}

func TestWebhookPoller_GracefulShutdown(t *testing.T) {
	addr := freePort(t)
	b := webhookTestBot(t)

	updates := make(chan any, 10)
	stop := make(chan struct{})

	poller := &WebhookPoller{Listen: addr}

	done := make(chan struct{})
	go func() {
		poller.Poll(b, updates, stop)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)

	close(stop)

	select {
	case <-done:
		// Poll returned — good.
	case <-time.After(3 * time.Second):
		t.Fatal("Poll did not return after stop")
	}

	// updates channel should be closed.
	_, open := <-updates
	if open {
		t.Error("updates channel should be closed after Poll returns")
	}
}

func TestWebhookPoller_CustomServer(t *testing.T) {
	addr := freePort(t)
	b := webhookTestBot(t)

	updates := make(chan any, 10)
	stop := make(chan struct{})

	srv := &http.Server{
		Addr:              addr,
		ReadHeaderTimeout: 3 * time.Second,
	}

	poller := &WebhookPoller{Server: srv}
	go poller.Poll(b, updates, stop)
	time.Sleep(50 * time.Millisecond)

	body := `{"update_type":"bot_started","timestamp":1,"chat_id":1,"user":{"user_id":1,"first_name":"T","is_bot":false,"last_activity_time":0}}`
	resp, err := http.Post("http://"+addr+"/", "application/json", strings.NewReader(body))
	if err != nil {
		close(stop)
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	select {
	case upd := <-updates:
		if _, ok := upd.(*maxigo.BotStartedUpdate); !ok {
			t.Errorf("expected *maxigo.BotStartedUpdate, got %T", upd)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")
	}

	close(stop)
	for range updates {
	}
}

func TestWebhookPoller_PanicRoutesToOnError(t *testing.T) {
	b := webhookTestBot(t)

	var mu sync.Mutex
	var gotErr error
	b.OnError = func(err error, c Context) {
		mu.Lock()
		defer mu.Unlock()
		gotErr = err
	}

	updates := make(chan any, 10)
	stop := make(chan struct{})

	// Invalid listen address triggers a server error.
	poller := &WebhookPoller{Listen: ":-1"}
	poller.Poll(b, updates, stop)

	mu.Lock()
	defer mu.Unlock()
	if gotErr == nil {
		t.Fatal("expected an error for invalid listen address")
	}
	if !strings.Contains(gotErr.Error(), "webhook: server error") {
		t.Errorf("expected 'webhook: server error', got: %v", gotErr)
	}
}

func TestWebhookPoller_BodySizeLimit(t *testing.T) {
	addr := freePort(t)
	b := webhookTestBot(t)

	var mu sync.Mutex
	var gotErr error
	b.OnError = func(err error, c Context) {
		mu.Lock()
		defer mu.Unlock()
		if gotErr == nil {
			gotErr = err
		}
	}

	updates := make(chan any, 10)
	stop := make(chan struct{})

	poller := &WebhookPoller{Listen: addr}
	go poller.Poll(b, updates, stop)
	time.Sleep(50 * time.Millisecond)

	// Send a body larger than 1MB — it will be truncated and fail to parse.
	bigBody := bytes.Repeat([]byte("x"), webhookMaxBodySize+1)
	resp, err := http.Post("http://"+addr+"/", "application/json", bytes.NewReader(bigBody))
	if err != nil {
		close(stop)
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}

	close(stop)
	for range updates {
	}
}

func TestWithPoller(t *testing.T) {
	mock := &mockPoller{}

	b, err := New("test-token", WithPoller(mock))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Should complete immediately — mockPoller sends no updates and waits for stop.
	go func() {
		time.Sleep(50 * time.Millisecond)
		b.Stop()
	}()
	b.Start()

	if _, ok := b.poller.(*mockPoller); !ok {
		t.Errorf("expected *mockPoller, got %T", b.poller)
	}
}

func TestWithPoller_OverridesLongPolling(t *testing.T) {
	mock := &mockPoller{}

	// WithLongPolling set first, then overridden by WithPoller.
	b, err := New("test-token", WithLongPolling(30), WithPoller(mock))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, ok := b.poller.(*mockPoller); !ok {
		t.Errorf("expected *mockPoller, got %T", b.poller)
	}
}

func TestWebhookPoller_IntegrationWithBot(t *testing.T) {
	addr := freePort(t)

	var mu sync.Mutex
	var handled bool
	var handledUserID int64

	poller := &WebhookPoller{Listen: addr}
	b, err := New("test-token", WithPoller(poller))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	b.Handle(OnBotStarted, func(c Context) error {
		mu.Lock()
		handled = true
		handledUserID = c.Sender().UserID
		mu.Unlock()
		return nil
	})

	go b.Start()
	time.Sleep(50 * time.Millisecond)

	body := `{"update_type":"bot_started","timestamp":1,"chat_id":1,"user":{"user_id":42,"first_name":"Test","is_bot":false,"last_activity_time":0}}`
	resp, err := http.Post("http://"+addr+"/", "application/json", strings.NewReader(body))
	if err != nil {
		b.Stop()
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()

	// Wait for handler to process.
	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		done := handled
		mu.Unlock()
		if done {
			break
		}
		select {
		case <-deadline:
			b.Stop()
			t.Fatal("timed out waiting for handler")
		case <-time.After(10 * time.Millisecond):
		}
	}

	mu.Lock()
	if handledUserID != 42 {
		t.Errorf("user_id = %d, want 42", handledUserID)
	}
	mu.Unlock()

	b.Stop()
}

// Verify WebhookPoller implements Poller.
var _ Poller = (*WebhookPoller)(nil)

func TestWebhookPoller_ConcurrentRequests(t *testing.T) {
	addr := freePort(t)
	b := webhookTestBot(t)

	updates := make(chan any, 100)
	stop := make(chan struct{})

	poller := &WebhookPoller{Listen: addr}
	go poller.Poll(b, updates, stop)
	time.Sleep(50 * time.Millisecond)

	const n = 20
	var wg sync.WaitGroup
	wg.Add(n)

	for i := range n {
		go func(id int) {
			defer wg.Done()
			body := fmt.Sprintf(`{"update_type":"bot_started","timestamp":%d,"chat_id":%d,"user":{"user_id":%d,"first_name":"T","is_bot":false,"last_activity_time":0}}`, id, id, id)
			resp, err := http.Post("http://"+addr+"/", "application/json", strings.NewReader(body))
			if err != nil {
				t.Errorf("request %d: %v", id, err)
				return
			}
			resp.Body.Close()
		}(i)
	}

	wg.Wait()

	received := 0
	timeout := time.After(3 * time.Second)
	for received < n {
		select {
		case <-updates:
			received++
		case <-timeout:
			close(stop)
			t.Fatalf("received %d/%d concurrent updates", received, n)
		}
	}

	close(stop)
	for range updates {
	}
}