package provider

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
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

func TestFetchEpisodePartialResponseCostsOneRequest(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		if r.URL.Query().Get("tmdb_id") == "" {
			t.Errorf("unexpected non-TMDB request: %s", r.URL.RawQuery)
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
	if response == nil || len(response.Intro) != 1 || len(response.Credits) != 0 {
		t.Fatalf("response = %#v, want partial TMDB response", response)
	}
	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Fatalf("server hits = %d, want 1", got)
	}
}

func TestFetchEpisodeMissCostsOneRequest(t *testing.T) {
	var queries []url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.Query())
		http.NotFound(w, r)
	}))
	defer srv.Close()

	c := NewClient("")
	c.SetBaseURL(srv.URL)
	response, err := c.FetchEpisode(context.Background(), "111", "222", "tt333", 1, 2, 0)
	if err != nil {
		t.Fatalf("FetchEpisode: %v", err)
	}
	if response != nil {
		t.Fatalf("response = %#v, want nil for a miss", response)
	}
	// Every 404 counts against the anonymous daily quota, so a miss must not
	// retry under the TVDB or IMDb identity.
	if len(queries) != 1 || queries[0].Get("tmdb_id") != "111" ||
		queries[0].Get("tvdb_id") != "" || queries[0].Get("imdb_id") != "" {
		t.Fatalf("queries = %#v, want one TMDB-only request", queries)
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
		w.Header().Set("Content-Type", "text/html; charset=UTF-8")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("<html>Cloudflare: Sorry, you have been blocked</html>"))
	}))
	defer srv.Close()

	c := NewClient("")
	c.SetBaseURL(srv.URL)

	_, err := c.FetchEpisode(context.Background(), "111", "222", "tt333", 1, 2, 0)
	var blocked *RetryAfterError
	if !errors.As(err, &blocked) || blocked.RetryAfter != defaultBlockedCooldown {
		t.Fatalf("error = %v, want host cooldown of %s", err, defaultBlockedCooldown)
	}
	if !strings.Contains(err.Error(), "Cloudflare blocked request") || !strings.Contains(err.Error(), "test-ray-SJC") ||
		strings.Contains(err.Error(), "<html>") {
		t.Fatalf("error = %q, want classified Cloudflare block with ray ID and no block page", err)
	}
	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Fatalf("server hits = %d, want 1", got)
	}
}

func TestFetchEpisodeReportsNonCloudflareForbiddenAsHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// A non-JSON 403 without Cloudflare provenance, e.g. from a proxy in
		// front of a self-hosted mirror.
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("forbidden by proxy"))
	}))
	defer srv.Close()

	c := NewClient("")
	c.SetBaseURL(srv.URL)
	_, err := c.FetchEpisode(context.Background(), "111", "", "", 1, 2, 0)
	var blocked *RetryAfterError
	if err == nil || errors.As(err, &blocked) || !strings.Contains(err.Error(), "HTTP 403: forbidden by proxy") {
		t.Fatalf("error = %v, want plain HTTP 403 without a provider cooldown", err)
	}
}

func TestFetchEpisodeReportsOriginForbiddenAsHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Cloudflare fronts every API response, including origin JSON errors.
		w.Header().Set("Server", "cloudflare")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":"forbidden"}`))
	}))
	defer srv.Close()

	c := NewClient("")
	c.SetBaseURL(srv.URL)
	_, err := c.FetchEpisode(context.Background(), "111", "", "", 1, 2, 0)
	var blocked *RetryAfterError
	if err == nil || errors.As(err, &blocked) || !strings.Contains(err.Error(), `HTTP 403: {"error":"forbidden"}`) {
		t.Fatalf("error = %v, want origin HTTP 403 without a provider cooldown", err)
	}
}

func TestResponseCacheTTLUsesShortTTLUntilIntroAndCreditsExist(t *testing.T) {
	ms := func(v int64) *int64 { return &v }
	intro := segmentTimestamps{StartMs: ms(5_000), EndMs: ms(60_000)}
	credits := segmentTimestamps{StartMs: ms(1_200_000), EndMs: ms(1_260_000)}
	openEndedCredits := segmentTimestamps{StartMs: ms(1_200_000)}
	// TheIntroDB's no-credits sentinel: an entry that convertMarkers discards.
	noCredits := segmentTimestamps{StartMs: ms(0)}
	for _, test := range []struct {
		name     string
		response *mediaResponse
		want     time.Duration
	}{
		{name: "missing response", response: nil, want: defaultIncompleteCacheTTL},
		{name: "intro only", response: &mediaResponse{Intro: []segmentTimestamps{intro}}, want: defaultIncompleteCacheTTL},
		{name: "no-credits sentinel", response: &mediaResponse{Intro: []segmentTimestamps{intro}, Credits: []segmentTimestamps{noCredits}}, want: defaultIncompleteCacheTTL},
		{name: "empty entries", response: &mediaResponse{Intro: []segmentTimestamps{{}}, Credits: []segmentTimestamps{{}}}, want: defaultIncompleteCacheTTL},
		{name: "complete", response: &mediaResponse{Intro: []segmentTimestamps{intro}, Credits: []segmentTimestamps{credits}}, want: defaultCacheTTL},
		{name: "credits to the end of the file", response: &mediaResponse{Intro: []segmentTimestamps{intro}, Credits: []segmentTimestamps{openEndedCredits}}, want: defaultCacheTTL},
	} {
		if got := responseCacheTTL(test.response); got != test.want {
			t.Errorf("%s: TTL = %s, want %s", test.name, got, test.want)
		}
	}
}

// trackedBody records how much of a response body was consumed and whether it
// was closed, so the status branches can be checked without a live server.
type trackedBody struct {
	reader io.Reader
	read   int
	closed bool
}

func (b *trackedBody) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	b.read += n
	return n, err
}

func (b *trackedBody) Close() error {
	b.closed = true
	return nil
}

func TestCloseResponseDrainsAndClosesBody(t *testing.T) {
	body := &trackedBody{reader: strings.NewReader(strings.Repeat("x", 64))}
	closeResponse(&http.Response{Body: body})

	if !body.closed {
		t.Error("response body was not closed")
	}
	if body.read != 64 {
		t.Errorf("drained %d bytes, want 64 so the connection stays reusable", body.read)
	}
}

func TestCloseResponseToleratesMissingBody(t *testing.T) {
	closeResponse(nil)
	closeResponse(&http.Response{})
}
