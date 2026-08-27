package provider

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFetchEpisodeSendsTVDBWhenNoTMDB(t *testing.T) {
	var gotQuery url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query()
		_, _ = w.Write([]byte(`{"type":"episode"}`))
	}))
	defer srv.Close()

	c := NewClient("")
	c.SetBaseURL(srv.URL)
	if _, err := c.FetchEpisode(context.Background(), "", "55555", "tt1234567", 2, 3, 0); err != nil {
		t.Fatalf("FetchEpisode: %v", err)
	}
	if gotQuery.Get("tvdb_id") != "55555" {
		t.Errorf("tvdb_id = %q, want 55555", gotQuery.Get("tvdb_id"))
	}
	if gotQuery.Get("tmdb_id") != "" {
		t.Errorf("tmdb_id = %q, want empty", gotQuery.Get("tmdb_id"))
	}
	if gotQuery.Get("imdb_id") != "" {
		t.Errorf("imdb_id should be omitted when tvdb present, got %q", gotQuery.Get("imdb_id"))
	}
}

func TestFetchEpisodePrefersTMDBOverTVDBAndIMDB(t *testing.T) {
	var gotQuery url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query()
		_, _ = w.Write([]byte(`{"type":"episode"}`))
	}))
	defer srv.Close()

	c := NewClient("")
	c.SetBaseURL(srv.URL)
	if _, err := c.FetchEpisode(context.Background(), "111", "222", "tt333", 1, 1, 0); err != nil {
		t.Fatalf("FetchEpisode: %v", err)
	}
	if gotQuery.Get("tmdb_id") != "111" {
		t.Errorf("tmdb_id = %q, want 111", gotQuery.Get("tmdb_id"))
	}
	if gotQuery.Get("tvdb_id") != "" || gotQuery.Get("imdb_id") != "" {
		t.Errorf("only tmdb_id expected, got tvdb=%q imdb=%q", gotQuery.Get("tvdb_id"), gotQuery.Get("imdb_id"))
	}
}

func TestFetchMovieSendsTVDB(t *testing.T) {
	var gotQuery url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query()
		_, _ = w.Write([]byte(`{"type":"movie"}`))
	}))
	defer srv.Close()

	c := NewClient("")
	c.SetBaseURL(srv.URL)
	if _, err := c.FetchMovie(context.Background(), "", "888", "", 0); err != nil {
		t.Fatalf("FetchMovie: %v", err)
	}
	if gotQuery.Get("tvdb_id") != "888" {
		t.Errorf("tvdb_id = %q, want 888", gotQuery.Get("tvdb_id"))
	}
}

func TestFetchEpisodeCachesByID(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hits, 1)
		_, _ = w.Write([]byte(`{"type":"episode"}`))
	}))
	defer srv.Close()

	c := NewClient("")
	c.SetBaseURL(srv.URL)
	for i := 0; i < 3; i++ {
		if _, err := c.FetchEpisode(context.Background(), "", "999", "", 1, 1, 0); err != nil {
			t.Fatalf("FetchEpisode: %v", err)
		}
	}
	if hits != 1 {
		t.Fatalf("server hits = %d, want 1 (cached after first)", hits)
	}
}

func TestFetchEpisodeCollapsesConcurrentLookups(t *testing.T) {
	var hits int32
	requestStarted := make(chan struct{}, 1)
	releaseRequest := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hits, 1)
		select {
		case requestStarted <- struct{}{}:
		default:
		}
		<-releaseRequest
		_, _ = w.Write([]byte(`{"type":"episode","intro":[{"end_ms":60000}]}`))
	}))
	defer srv.Close()

	c := NewClient("")
	t.Cleanup(c.Close)
	c.SetBaseURL(srv.URL)

	const callers = 12
	start := make(chan struct{})
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := c.FetchEpisode(context.Background(), "111", "", "", 1, 2, 0)
			errs <- err
		}()
	}
	close(start)
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("first lookup did not reach server")
	}
	close(releaseRequest)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("FetchEpisode: %v", err)
		}
	}
	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Fatalf("server hits = %d, want 1 collapsed lookup", got)
	}
}

func TestFetchEpisodeFallsBackThroughTVDBToIMDB(t *testing.T) {
	var queries []url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.Query())
		if r.URL.Query().Get("imdb_id") == "tt333" {
			_, _ = w.Write([]byte(`{"type":"episode","credits":[{"start_ms":1200000}]}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	c := NewClient("")
	t.Cleanup(c.Close)
	c.SetBaseURL(srv.URL)
	response, err := c.FetchEpisode(context.Background(), "111", "222", "tt333", 1, 2, 0)
	if err != nil {
		t.Fatalf("FetchEpisode: %v", err)
	}
	if response == nil || len(response.Credits) != 1 {
		t.Fatalf("response = %#v, want IMDb credits marker", response)
	}
	if len(queries) != 3 || queries[0].Get("tmdb_id") != "111" ||
		queries[1].Get("tvdb_id") != "222" || queries[2].Get("imdb_id") != "tt333" {
		t.Fatalf("queries = %#v, want TMDB, TVDB, then IMDb", queries)
	}
}

func TestFetchEpisodeStopsFallbackAfterPartialResponse(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		if r.URL.Query().Get("tmdb_id") == "" {
			t.Fatalf("unexpected alternate-ID request: %s", r.URL.RawQuery)
		}
		_, _ = w.Write([]byte(`{"type":"episode","intro":[{"end_ms":60000}]}`))
	}))
	defer srv.Close()

	c := NewClient("")
	t.Cleanup(c.Close)
	c.SetBaseURL(srv.URL)
	response, err := c.FetchEpisode(context.Background(), "111", "222", "tt333", 1, 2, 0)
	if err != nil {
		t.Fatalf("FetchEpisode: %v", err)
	}
	if response == nil || len(response.Intro) != 1 || len(response.Credits) != 0 {
		t.Fatalf("response = %#v, want partial TMDB response", response)
	}
	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Fatalf("server hits = %d, want no fallback after a real response", got)
	}
}

func TestFetchEpisodeFallsBackFromTMDBNotFoundToTVDB(t *testing.T) {
	var queries []url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.Query())
		if r.URL.Query().Get("tmdb_id") != "" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"type":"episode","intro":[{"end_ms":60000}]}`))
	}))
	defer srv.Close()

	c := NewClient("")
	c.SetBaseURL(srv.URL)
	response, err := c.FetchEpisode(context.Background(), "111", "222", "tt333", 1, 2, 0)
	if err != nil {
		t.Fatalf("FetchEpisode: %v", err)
	}
	if response == nil || len(response.Intro) != 1 {
		t.Fatalf("response = %#v, want TVDB intro marker", response)
	}
	if len(queries) != 2 {
		t.Fatalf("queries = %d, want TMDB then TVDB", len(queries))
	}
	if got := queries[0].Get("tmdb_id"); got != "111" {
		t.Fatalf("first tmdb_id = %q, want 111", got)
	}
	if got := queries[1].Get("tvdb_id"); got != "222" {
		t.Fatalf("second tvdb_id = %q, want 222", got)
	}
}

func TestFetchEpisodeStopsAndCoolsDownAfterCloudflareForbidden(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		if got := r.Header.Get("User-Agent"); got != clientUserAgent {
			t.Errorf("User-Agent = %q, want %q", got, clientUserAgent)
		}
		w.Header().Set("Server", "cloudflare")
		w.Header().Set("CF-Ray", "test-ray-SJC")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("<html>Cloudflare: Sorry, you have been blocked</html>"))
	}))
	defer srv.Close()

	c := NewClient("")
	t.Cleanup(c.Close)
	c.SetBaseURL(srv.URL)

	_, err := c.FetchEpisode(context.Background(), "111", "222", "tt333", 1, 2, 0)
	if err == nil || !strings.Contains(err.Error(), "Cloudflare blocked request") ||
		!strings.Contains(err.Error(), "test-ray-SJC") {
		t.Fatalf("first error = %v, want classified Cloudflare block with ray ID", err)
	}
	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Fatalf("server hits after fallback candidates = %d, want 1", got)
	}

	_, err = c.FetchEpisode(context.Background(), "444", "555", "tt666", 3, 4, 0)
	if err == nil || !strings.Contains(err.Error(), "requests paused") {
		t.Fatalf("cooldown error = %v, want paused request", err)
	}
	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Fatalf("server hits during cooldown = %d, want 1", got)
	}
}

func TestResponseCacheTTLUsesShortTTLUntilIntroAndCreditsExist(t *testing.T) {
	c := NewClient("")
	t.Cleanup(c.Close)

	intro := segmentTimestamps{}
	credits := segmentTimestamps{}
	if got := c.responseCacheTTL(&mediaResponse{Intro: []segmentTimestamps{intro}}); got != defaultIncompleteCacheTTL {
		t.Fatalf("partial response TTL = %s, want %s", got, defaultIncompleteCacheTTL)
	}
	if got := c.responseCacheTTL(&mediaResponse{
		Intro: []segmentTimestamps{intro}, Credits: []segmentTimestamps{credits},
	}); got != defaultCacheTTL {
		t.Fatalf("complete response TTL = %s, want %s", got, defaultCacheTTL)
	}
}
