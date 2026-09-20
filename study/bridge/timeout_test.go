package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestInfluxRequestUsesContextDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(21 * time.Second):
			w.WriteHeader(http.StatusNoContent)
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	client := newInfluxClient(server.URL)
	defer client.Close()

	for _, test := range []struct {
		name        string
		deadline    time.Duration
		wantTimeout bool
	}{
		{"past SDK default", 30 * time.Second, false},
		{"context still cancels", 50 * time.Millisecond, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), test.deadline)
			defer cancel()
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			response, err := client.Options().HTTPClient().Do(request)
			if test.wantTimeout {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("expected deadline, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if _, err := io.Copy(io.Discard, response.Body); err != nil {
				t.Fatal(err)
			}
		})
	}
}
