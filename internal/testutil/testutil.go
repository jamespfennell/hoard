package testutil

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"testing"
	"time"
)

func ErrorOrFail(t *testing.T, err error) {
	if err != nil {
		t.Fatalf("Unexpected error '%s'", err)
	}
}

type FeedServer struct {
	listener      net.Listener
	server        *http.Server
	closedServerC chan struct{}
	responses     map[string]bool
}

func NewFeedServer() (*FeedServer, error) {
	f := FeedServer{
		closedServerC: make(chan struct{}),
		responses:     map[string]bool{},
	}
	var err error
	f.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		return nil, err
	}
	http.HandleFunc("/", func(writer http.ResponseWriter, req *http.Request) {
		response := randSeq(20)
		f.responses[response] = true
		_, _ = writer.Write([]byte(response))
		fmt.Println("Sent response", response)
	})
	f.server = &http.Server{}
	rand.Seed(time.Now().UnixNano())
	go func() {
		if err := f.server.Serve(f.listener); err != nil {
			fmt.Printf("HTTP server stopped: %s\n", err)
		}
		close(f.closedServerC)
	}()
	return &f, nil
}

func (f *FeedServer) Port() int {
	return f.listener.Addr().(*net.TCPAddr).Port
}

func (f *FeedServer) Shutdown() error {
	err := f.server.Shutdown(context.Background())
	<-f.closedServerC
	return err
}

func (f *FeedServer) Responses() map[string]bool {
	return f.responses
}

// Source:
// https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-go
func randSeq(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
