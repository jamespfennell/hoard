package testutil

import (
	"context"
	"fmt"
	"github.com/jamespfennell/hoard/internal/storage"
	"github.com/jamespfennell/hoard/internal/storage/hour"
	"math/rand"
	"net"
	"net/http"
	"testing"
	"time"
)

var Data = []struct {
	Content []byte
	DFile   storage.DFile
	Hour    hour.Hour
}{
	{
		[]byte{50, 51, 52},
		storage.DFile{
			Prefix:  "",
			Postfix: "",
			Time:    time.Date(2000, 1, 2, 3, 4, 5, 0, time.UTC),
			Hash:    storage.CalculateHash([]byte{50, 51, 52}),
		},
		hour.Date(2000, 1, 2, 3),
	},
	{
		[]byte{60, 61, 62},
		storage.DFile{
			Prefix:  "",
			Postfix: "",
			Time:    time.Date(2000, 1, 2, 3, 5, 5, 0, time.UTC),
			Hash:    storage.CalculateHash([]byte{60, 61, 62}),
		},
		hour.Date(2000, 1, 2, 3),
	},
	{
		[]byte{60, 61, 62},
		storage.DFile{
			Prefix:  "",
			Postfix: "",
			Time:    time.Date(2000, 1, 2, 3, 6, 10, 0, time.UTC),
			Hash:    storage.CalculateHash([]byte{60, 61, 62}),
		},
		hour.Date(2000, 1, 2, 3),
	},
	{
		[]byte{70, 71, 72},
		storage.DFile{
			Prefix:  "",
			Postfix: "",
			Time:    time.Date(2000, 1, 2, 3, 6, 15, 0, time.UTC),
			Hash:    storage.CalculateHash([]byte{70, 71, 72}),
		},
		hour.Date(2000, 1, 2, 3),
	},
}

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
