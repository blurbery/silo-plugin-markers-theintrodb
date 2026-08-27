package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const (
	maxRetries      = 3
	maxResponseBody = 1 << 20 // 1 MB
	defaultTimeout  = 15 * time.Second
	defaultCacheTTL = 24 * time.Hour
	clientUserAgent = "Silo-Server-TheIntroDB-Plugin/1.0 (+https://github.com/Silo-Server/silo-plugin-markers-theintrodb)"
	// Missing and partial responses are deliberately short-lived. A later
	// playback should be able to discover newly contributed intro or credits
	// markers without hammering TheIntroDB on repeated starts.
	defaultIncompleteCacheTTL = 15 * time.Minute
	// A Cloudflare block is not content-specific. Continuing with alternate IDs
	// only multiplies rejected traffic and can prolong an automated block.
	defaultBlockedCooldown = 5 * time.Minute
)

// Client is an HTTP client for the TheIntroDB /v3/media endpoint. Each
// instance has its own rate limiter and response cache; concurrent fetches
// for the same lookup key collapse to a single HTTP round trip via the cache.
type Client struct {
	httpClient         *http.Client
	mu                 sync.RWMutex
	apiKey             string
	baseURL            string
	limiter            *rate.Limiter
	cache              *ttlCache[*mediaResponse]
	cacheTTL           time.Duration
	incompleteCacheTTL time.Duration
	blockedUntil       time.Time
	inflightMu         sync.Mutex
	inflight           map[string]*inflightCall
}

type inflightCall struct {
	done     chan struct{}
	response *mediaResponse
	err      error
}

// NewClient builds a Client with the canonical rate limit and cache TTL.
// The apiKey may be empty — TheIntroDB serves read traffic without a key,
// the key only gates access to the caller's own pending submissions.
func NewClient(apiKey string) *Client {
	return &Client{
		httpClient: &http.Client{Timeout: defaultTimeout},
		apiKey:     strings.TrimSpace(apiKey),
		baseURL:    DefaultBaseURL,
		// TheIntroDB documents 30 requests / 10 seconds per IP. We stay
		// conservatively below that: 2 req/s sustained, burst 5.
		limiter:            rate.NewLimiter(2, 5),
		cache:              newTTLCache[*mediaResponse](),
		cacheTTL:           defaultCacheTTL,
		incompleteCacheTTL: defaultIncompleteCacheTTL,
		inflight:           make(map[string]*inflightCall),
	}
}

// SetBaseURL overrides the API base URL (used by tests).
func (c *Client) SetBaseURL(u string) {
	c.mu.Lock()
	c.baseURL = u
	c.mu.Unlock()
}

// SetAPIKey rotates the bearer token in-place. Safe to call concurrently
// with in-flight requests; subsequent requests use the new key.
func (c *Client) SetAPIKey(apiKey string) {
	c.mu.Lock()
	c.apiKey = strings.TrimSpace(apiKey)
	c.mu.Unlock()
}

// Close releases the background sweeper goroutine inside the response cache.
func (c *Client) Close() {
	if c.cache != nil {
		c.cache.Close()
	}
}

// FetchEpisode looks up segment timestamps for a TV episode.
// At least one of tmdbID, tvdbID, or imdbID must be non-empty. When several are
// present the preference order is tmdb → tvdb → imdb (matching TheIntroDB's own
// clients).
func (c *Client) FetchEpisode(ctx context.Context, tmdbID, tvdbID, imdbID string, season, episode int, durationMS int64) (*mediaResponse, error) {
	if tmdbID == "" && tvdbID == "" && imdbID == "" {
		return nil, fmt.Errorf("introdb: tmdb_id, tvdb_id, or imdb_id required")
	}
	if season <= 0 || episode <= 0 {
		return nil, fmt.Errorf("introdb: episode lookup requires season and episode > 0 (got %d/%d)", season, episode)
	}
	return c.fetchUsingIDs(ctx, tmdbID, tvdbID, imdbID, func(id externalIDCandidate) (url.Values, string) {
		q := id.query()
		q.Set("season", strconv.Itoa(season))
		q.Set("episode", strconv.Itoa(episode))
		if durationMS > 0 {
			q.Set("duration_ms", strconv.FormatInt(durationMS, 10))
		}
		return q, "episode:" + q.Encode()
	})
}

// FetchMovie looks up segment timestamps for a movie.
// At least one of tmdbID, tvdbID, or imdbID must be non-empty.
func (c *Client) FetchMovie(ctx context.Context, tmdbID, tvdbID, imdbID string, durationMS int64) (*mediaResponse, error) {
	if tmdbID == "" && tvdbID == "" && imdbID == "" {
		return nil, fmt.Errorf("introdb: tmdb_id, tvdb_id, or imdb_id required")
	}
	return c.fetchUsingIDs(ctx, tmdbID, tvdbID, imdbID, func(id externalIDCandidate) (url.Values, string) {
		q := id.query()
		if durationMS > 0 {
			q.Set("duration_ms", strconv.FormatInt(durationMS, 10))
		}
		return q, "movie:" + q.Encode()
	})
}

type externalIDCandidate struct {
	key   string
	value string
}

func (id externalIDCandidate) query() url.Values {
	return url.Values{id.key: []string{id.value}}
}

func externalIDCandidates(tmdbID, tvdbID, imdbID string) []externalIDCandidate {
	ids := make([]externalIDCandidate, 0, 3)
	if tmdbID != "" {
		ids = append(ids, externalIDCandidate{key: "tmdb_id", value: tmdbID})
	}
	if tvdbID != "" {
		ids = append(ids, externalIDCandidate{key: "tvdb_id", value: tvdbID})
	}
	if imdbID != "" {
		ids = append(ids, externalIDCandidate{key: "imdb_id", value: imdbID})
	}
	return ids
}

// fetchUsingIDs tries identifiers in TMDB, TVDB, IMDb order. A 404 advances
// to the next identity because TheIntroDB can have a record indexed under one
// provider but not another. The first actual media response wins.
func (c *Client) fetchUsingIDs(
	ctx context.Context,
	tmdbID, tvdbID, imdbID string,
	request func(externalIDCandidate) (url.Values, string),
) (*mediaResponse, error) {
	for _, id := range externalIDCandidates(tmdbID, tvdbID, imdbID) {
		q, key := request(id)
		response, err := c.fetch(ctx, q, key)
		if err != nil {
			// Alternate identifiers help only when a particular identity is not
			// indexed. They cannot recover transport, rate-limit, or WAF errors.
			return nil, err
		}
		if response != nil {
			return response, nil
		}
	}
	return nil, nil
}

func (c *Client) fetch(ctx context.Context, q url.Values, key string) (*mediaResponse, error) {
	if cached, ok := c.cache.Get(key); ok {
		return cached, nil
	}
	if remaining := c.blockedCooldownRemaining(); remaining > 0 {
		return nil, fmt.Errorf("introdb: requests paused after Cloudflare HTTP 403; retry in %s", remaining.Round(time.Second))
	}

	c.inflightMu.Lock()
	if call, ok := c.inflight[key]; ok {
		c.inflightMu.Unlock()
		select {
		case <-call.done:
			return call.response, call.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	call := &inflightCall{done: make(chan struct{})}
	c.inflight[key] = call
	c.inflightMu.Unlock()

	response, err := c.fetchUncached(ctx, q, key)
	c.inflightMu.Lock()
	call.response = response
	call.err = err
	close(call.done)
	delete(c.inflight, key)
	c.inflightMu.Unlock()
	return response, err
}

func (c *Client) fetchUncached(ctx context.Context, q url.Values, key string) (*mediaResponse, error) {
	if err := c.limiter.Wait(ctx); err != nil {
		return nil, err
	}

	c.mu.RLock()
	baseURL := c.baseURL
	apiKey := c.apiKey
	c.mu.RUnlock()

	reqURL := baseURL + "/media?" + q.Encode()

	for attempt := 0; attempt <= maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return nil, fmt.Errorf("introdb: create request: %w", err)
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", clientUserAgent)
		if apiKey != "" {
			req.Header.Set("Authorization", "Bearer "+apiKey)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("introdb: request failed: %w", err)
		}

		if resp.StatusCode == http.StatusNotFound {
			resp.Body.Close()
			// Cache negatives too so the next playback start doesn't trigger
			// another fetch for known-empty content.
			c.cache.Set(key, nil, c.incompleteCacheTTL)
			return nil, nil
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			resp.Body.Close()
			if attempt < maxRetries {
				backoff := retryAfterOrDefault(resp, attempt)
				select {
				case <-time.After(backoff):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				continue
			}
			return nil, fmt.Errorf("introdb: rate limited after %d retries", maxRetries)
		}

		if resp.StatusCode >= 500 {
			resp.Body.Close()
			if attempt < maxRetries {
				backoff := time.Duration(1<<attempt) * time.Second
				select {
				case <-time.After(backoff):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				continue
			}
			return nil, fmt.Errorf("introdb: server error %d after %d retries", resp.StatusCode, maxRetries)
		}

		if resp.StatusCode >= 400 {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			if resp.StatusCode == http.StatusForbidden &&
				(strings.EqualFold(resp.Header.Get("Server"), "cloudflare") || bytes.Contains(bytes.ToLower(body), []byte("cloudflare"))) {
				c.pauseBlockedRequests(defaultBlockedCooldown)
				ray := strings.TrimSpace(resp.Header.Get("CF-Ray"))
				if ray != "" {
					return nil, fmt.Errorf("introdb: Cloudflare blocked request (HTTP 403, cf-ray %s); paused requests for %s", ray, defaultBlockedCooldown)
				}
				return nil, fmt.Errorf("introdb: Cloudflare blocked request (HTTP 403); paused requests for %s", defaultBlockedCooldown)
			}
			return nil, fmt.Errorf("introdb: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}

		var out mediaResponse
		decodeErr := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBody)).Decode(&out)
		resp.Body.Close()
		if decodeErr != nil {
			return nil, fmt.Errorf("introdb: decode response: %w", decodeErr)
		}
		c.cache.Set(key, &out, c.responseCacheTTL(&out))
		return &out, nil
	}
	return nil, fmt.Errorf("introdb: max retries exceeded")
}

func (c *Client) pauseBlockedRequests(cooldown time.Duration) {
	c.mu.Lock()
	until := time.Now().Add(cooldown)
	if until.After(c.blockedUntil) {
		c.blockedUntil = until
	}
	c.mu.Unlock()
}

func (c *Client) blockedCooldownRemaining() time.Duration {
	c.mu.RLock()
	remaining := time.Until(c.blockedUntil)
	c.mu.RUnlock()
	if remaining < 0 {
		return 0
	}
	return remaining
}

func (c *Client) responseCacheTTL(response *mediaResponse) time.Duration {
	if response == nil || len(response.Intro) == 0 || len(response.Credits) == 0 {
		return c.incompleteCacheTTL
	}
	return c.cacheTTL
}

func retryAfterOrDefault(resp *http.Response, attempt int) time.Duration {
	if val := resp.Header.Get("Retry-After"); val != "" {
		if secs, err := strconv.Atoi(val); err == nil && secs > 0 {
			return time.Duration(secs) * time.Second
		}
	}
	return time.Duration(1<<attempt) * time.Second
}

// submitSegment contributes a single segment via POST /v3/submit. The API key
// is required (submissions are credited to that account); returns an error if
// none is configured. Submissions are not cached. On 429 the usage-limit reset
// is surfaced in the error so callers can back off.
func (c *Client) submitSegment(ctx context.Context, body submitRequest) (*submitResponse, error) {
	c.mu.RLock()
	baseURL := c.baseURL
	apiKey := c.apiKey
	c.mu.RUnlock()
	if apiKey == "" {
		return nil, fmt.Errorf("introdb: submit requires an API key")
	}

	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("introdb: marshal submit: %w", err)
	}
	if err := c.limiter.Wait(ctx); err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/submit", bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("introdb: create submit request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", clientUserAgent)
	req.Header.Set("Authorization", "Bearer "+apiKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("introdb: submit request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		after := time.Duration(usageResetSeconds(resp)) * time.Second
		return nil, &RetryAfterError{
			RetryAfter: after,
			Message:    fmt.Sprintf("introdb: submit usage-limited; retry after %s", after),
		}
	}
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))
		return nil, fmt.Errorf("introdb: submit HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}

	var out submitResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBody)).Decode(&out); err != nil {
		return nil, fmt.Errorf("introdb: decode submit response: %w", err)
	}
	return &out, nil
}

// fetchUserStats validates the configured key and returns contribution stats
// via GET /v3/user/stats.
func (c *Client) fetchUserStats(ctx context.Context) (*userStatsResponse, error) {
	c.mu.RLock()
	baseURL := c.baseURL
	apiKey := c.apiKey
	c.mu.RUnlock()
	if apiKey == "" {
		return nil, fmt.Errorf("introdb: user stats require an API key")
	}
	if err := c.limiter.Wait(ctx); err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/user/stats", nil)
	if err != nil {
		return nil, fmt.Errorf("introdb: create stats request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", clientUserAgent)
	req.Header.Set("Authorization", "Bearer "+apiKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("introdb: stats request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))
		return nil, fmt.Errorf("introdb: stats HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}

	var out userStatsResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBody)).Decode(&out); err != nil {
		return nil, fmt.Errorf("introdb: decode stats response: %w", err)
	}
	if out.Error != "" {
		return nil, fmt.Errorf("introdb: stats error: %s", out.Error)
	}
	return &out, nil
}

// usageResetSeconds reads the usage/rate reset hint from a 429 response.
func usageResetSeconds(resp *http.Response) int {
	for _, h := range []string{"X-UsageLimit-Reset", "X-RateLimit-Reset", "Retry-After"} {
		if v := resp.Header.Get(h); v != "" {
			if s, err := strconv.Atoi(v); err == nil && s > 0 {
				return s
			}
		}
	}
	return 0
}
