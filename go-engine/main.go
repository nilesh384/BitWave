package main

import (
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"math/bits"
	"net/http"
	"sort"
	"sync"
	"time"
)

type HLL struct {
	b         uint8
	m         int
	registers []uint8
	alpha     float64
}

func NewHLL(b uint8) *HLL {
	m := 1 << b
	return &HLL{
		b:         b,
		m:         m,
		registers: make([]uint8, m),
		alpha:     0.7213 / (1 + 1.079/float64(m)),
	}
}

func (h *HLL) hash(item string) uint64 {
	sum := sha1.Sum([]byte(item))
	return binary.BigEndian.Uint64(sum[:8])
}

func (h *HLL) rho(w uint64) uint8 {
	maxBits := 64 - int(h.b)
	if w == 0 {
		return uint8(maxBits + 1)
	}
	return uint8(maxBits - (64-bits.LeadingZeros64(w)) + 1)
}

func (h *HLL) Add(item string) {
	x := h.hash(item)
	j := x & uint64(h.m-1)
	w := x >> h.b
	rank := h.rho(w)
	if rank > h.registers[j] {
		h.registers[j] = rank
	}
}

func (h *HLL) Count() float64 {
	var z float64
	zeros := 0
	for _, r := range h.registers {
		z += math.Pow(2.0, -float64(r))
		if r == 0 {
			zeros++
		}
	}
	raw := h.alpha * float64(h.m*h.m) / z
	if raw <= 2.5*float64(h.m) && zeros > 0 {
		return float64(h.m) * math.Log(float64(h.m)/float64(zeros))
	}
	return raw
}

func (h *HLL) Merge(other *HLL) {
	if h.m != other.m {
		panic("cannot merge HLLs of different size")
	}
	for i := range h.registers {
		if other.registers[i] > h.registers[i] {
			h.registers[i] = other.registers[i]
		}
	}
}

type CMS struct {
	width  int
	depth  int
	table  [][]uint64
	seeds  []uint64
}

func NewCMS(width, depth int) *CMS {
	table := make([][]uint64, depth)
	for i := range table {
		table[i] = make([]uint64, width)
	}
	seeds := make([]uint64, depth)
	for i := range seeds {
		seeds[i] = uint64(i) * 0x9E3779B1
	}
	return &CMS{width: width, depth: depth, table: table, seeds: seeds}
}

func (c *CMS) hash(item string, seed uint64) int {
	h := sha1.Sum([]byte(fmt.Sprintf("%d-%s", seed, item)))
	return int(binary.BigEndian.Uint32(h[:4]) % uint32(c.width))
}

func (c *CMS) Add(item string, count uint64) {
	for row := 0; row < c.depth; row++ {
		col := c.hash(item, c.seeds[row])
		c.table[row][col] += count
	}
}

func (c *CMS) Estimate(item string) uint64 {
	min := ^uint64(0)
	for row := 0; row < c.depth; row++ {
		col := c.hash(item, c.seeds[row])
		if c.table[row][col] < min {
			min = c.table[row][col]
		}
	}
	return min
}

func (c *CMS) Merge(other *CMS) {
	for r := 0; r < c.depth; r++ {
		for col := 0; col < c.width; col++ {
			c.table[r][col] += other.table[r][col]
		}
	}
}

type TopKTracker struct {
	k          int
	candidates map[string]uint64
}

func NewTopKTracker(k int) *TopKTracker {
	return &TopKTracker{k: k, candidates: make(map[string]uint64)}
}

func (t *TopKTracker) Observe(item string, cms *CMS) {
	t.candidates[item] = cms.Estimate(item)
	if len(t.candidates) > t.k*5 {
		t.trim()
	}
}

func (t *TopKTracker) trim() {
	type pair struct {
		item  string
		count uint64
	}
	pairs := make([]pair, 0, len(t.candidates))
	for item, count := range t.candidates {
		pairs = append(pairs, pair{item: item, count: count})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].count > pairs[j].count })
	if len(pairs) > t.k {
		pairs = pairs[:t.k]
	}
	t.candidates = make(map[string]uint64, len(pairs))
	for _, pair := range pairs {
		t.candidates[pair.item] = pair.count
	}
}

type Bucket struct {
	start int64
	hll   *HLL
	cms   *CMS
	topk  *TopKTracker
}

type Engine struct {
	mu                sync.Mutex
	bucketSeconds     int64
	maxWindowBuckets  int64
	hllB              uint8
	cmsW              int
	cmsD              int
	buckets           []*Bucket
}

func NewEngine(bucketSeconds, maxWindowBuckets int64, hllB uint8, cmsW, cmsD int) *Engine {
	return &Engine{
		bucketSeconds:    bucketSeconds,
		maxWindowBuckets: maxWindowBuckets,
		hllB:             hllB,
		cmsW:             cmsW,
		cmsD:             cmsD,
		buckets:          []*Bucket{},
	}
}

func (e *Engine) currentBucketStart(ts float64) int64 {
	return int64(math.Floor(ts/float64(e.bucketSeconds))) * e.bucketSeconds
}

func (e *Engine) getOrCreateBucket(ts float64) *Bucket {
	bucketStart := e.currentBucketStart(ts)
	if len(e.buckets) > 0 && e.buckets[len(e.buckets)-1].start == bucketStart {
		return e.buckets[len(e.buckets)-1]
	}
	bucket := &Bucket{
		start: bucketStart,
		hll:   NewHLL(e.hllB),
		cms:   NewCMS(e.cmsW, e.cmsD),
		topk:  NewTopKTracker(10),
	}
	e.buckets = append(e.buckets, bucket)
	e.evictOld(ts)
	return bucket
}

func (e *Engine) evictOld(ts float64) {
	cutoff := e.currentBucketStart(ts) - e.maxWindowBuckets*e.bucketSeconds
	for len(e.buckets) > 0 && e.buckets[0].start < cutoff {
		e.buckets = e.buckets[1:]
	}
}

func (e *Engine) RecordEvent(userID, itemID string, ts float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	bucket := e.getOrCreateBucket(ts)
	bucket.hll.Add(userID)
	bucket.cms.Add(itemID, 1)
	bucket.topk.Observe(itemID, bucket.cms)
}

type TopItem [2]any

type QueryResult struct {
	UniqueUsersEstimate float64   `json:"unique_users_estimate"`
	TopItems            []TopItem `json:"top_items"`
}

func (e *Engine) QueryWindow(windowSeconds int, now float64) QueryResult {
	e.mu.Lock()
	defer e.mu.Unlock()

	cutoff := now - float64(windowSeconds)
	mergedHLL := NewHLL(e.hllB)
	mergedCMS := NewCMS(e.cmsW, e.cmsD)
	mergedCandidates := map[string]struct{}{}

	for _, bucket := range e.buckets {
		if float64(bucket.start) >= cutoff {
			mergedHLL.Merge(bucket.hll)
			mergedCMS.Merge(bucket.cms)
			for item := range bucket.topk.candidates {
				mergedCandidates[item] = struct{}{}
			}
		}
	}

	items := make([]TopItem, 0, len(mergedCandidates))
	for item := range mergedCandidates {
		items = append(items, TopItem{item, mergedCMS.Estimate(item)})
	}
	sort.Slice(items, func(i, j int) bool {
		left := items[i][1].(uint64)
		right := items[j][1].(uint64)
		if left == right {
			return items[i][0].(string) < items[j][0].(string)
		}
		return left > right
	})
	if len(items) > 5 {
		items = items[:5]
	}

	return QueryResult{
		UniqueUsersEstimate: mergedHLL.Count(),
		TopItems:            items,
	}
}

type recordRequest struct {
	UserID    string  `json:"user_id"`
	ItemID    string  `json:"item_id"`
	Timestamp float64 `json:"timestamp"`
}

type queryRequest struct {
	WindowSeconds int     `json:"window_seconds"`
	Now           float64 `json:"now"`
}

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	bucketSeconds := flag.Int64("bucket-seconds", 60, "bucket size in seconds")
	maxWindowBuckets := flag.Int64("max-window-buckets", 60, "number of live buckets to retain")
	hllB := flag.Uint("hll-b", 10, "hyperloglog register bits")
	cmsW := flag.Int("cms-w", 2000, "count-min sketch width")
	cmsD := flag.Int("cms-d", 5, "count-min sketch depth")
	flag.Parse()

	engine := NewEngine(*bucketSeconds, *maxWindowBuckets, uint8(*hllB), *cmsW, *cmsD)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/record", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req recordRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.Timestamp == 0 {
			req.Timestamp = float64(time.Now().Unix())
		}
		engine.RecordEvent(req.UserID, req.ItemID, req.Timestamp)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/query", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req queryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.Now == 0 {
			req.Now = float64(time.Now().Unix())
		}
		result := engine.QueryWindow(req.WindowSeconds, req.Now)
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(result); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	server := &http.Server{
		Addr:    *addr,
		Handler: mux,
	}

	log.Printf("Go engine listening on %s", *addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalln(err)
	}
}