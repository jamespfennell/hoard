package util

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// setUpPublicIPAddressTest resets the cached public IP address and replaces the lookup sites with test servers
// that respond using the provided handlers.
func setUpPublicIPAddressTest(t *testing.T, handlers ...http.HandlerFunc) {
	oldSites := publicIPAddressSites
	oldTimeout := publicIPAddressTimeout
	t.Cleanup(func() {
		publicIPAddressSites = oldSites
		publicIPAddressTimeout = oldTimeout
		publicIPAddress.value = nil
		publicIPAddress.lastFailure = time.Time{}
	})
	publicIPAddress.value = nil
	publicIPAddress.lastFailure = time.Time{}
	publicIPAddressTimeout = 100 * time.Millisecond
	publicIPAddressSites = nil
	for _, handler := range handlers {
		server := httptest.NewServer(handler)
		t.Cleanup(server.Close)
		publicIPAddressSites = append(publicIPAddressSites, server.URL)
	}
}

func respondWith(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(body))
	}
}

func TestGetPublicIPAddressOr_HangingSiteTimesOut(t *testing.T) {
	unblock := make(chan struct{})
	defer close(unblock)
	setUpPublicIPAddressTest(t,
		func(w http.ResponseWriter, r *http.Request) { <-unblock },
		respondWith("203.0.113.1\n"),
	)

	done := make(chan string, 1)
	go func() { done <- GetPublicIPAddressOr("default") }()
	select {
	case ip := <-done:
		if ip != "203.0.113.1" {
			t.Errorf("Unexpected IP address %q", ip)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Lookup did not time out")
	}
}

func TestGetPublicIPAddressOr_IPv6(t *testing.T) {
	setUpPublicIPAddressTest(t, respondWith("2001:db8:85a3::8a2e:370:7334\n"))

	if ip := GetPublicIPAddressOr("default"); ip != "2001:db8:85a3::8a2e:370:7334" {
		t.Errorf("Unexpected IP address %q", ip)
	}
}

func TestGetPublicIPAddressOr_RejectsNonIPResponse(t *testing.T) {
	setUpPublicIPAddressTest(t, respondWith("<b>error</b>"))

	if ip := GetPublicIPAddressOr("default"); ip != "default" {
		t.Errorf("Unexpected IP address %q", ip)
	}
}

func TestGetPublicIPAddressOr_FailureIsCached(t *testing.T) {
	numRequests := 0
	setUpPublicIPAddressTest(t, func(w http.ResponseWriter, r *http.Request) {
		numRequests++
		w.WriteHeader(http.StatusInternalServerError)
	})

	GetPublicIPAddressOr("default")
	if ip := GetPublicIPAddressOr("default"); ip != "default" {
		t.Errorf("Unexpected IP address %q", ip)
	}
	if numRequests != 1 {
		t.Errorf("Expected 1 request after a failed lookup; got %d", numRequests)
	}
}
