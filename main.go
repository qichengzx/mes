package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"github.com/elastic/go-elasticsearch/v6"
	"github.com/elastic/go-elasticsearch/v6/esapi"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	FLUSHBUFFER = 1000
)

var (
	u          string
	auth       string
	totalLines uint64
	hitResult  []hit
)

type options struct {
	print      bool
	outputFile string
	output     io.Writer

	//elasticsearch options
	address []string
	index   []string
	sort    []string
	docType string

	scroll time.Duration

	//The number of hits to return. Defaults to 1000.
	size      int
	maxResult int

	fields   []string
	q        string
	rawQuery bool
	//queryBody is elasticsearch query body
	queryBody *bytes.Buffer
	username  string
	password  string
}

type App struct {
	opts options

	totalLines  uint64
	queryResult uint64
	//numResult is actually result count to result.
	numResult uint64
	scrollIDs []string
	sIDs      map[string]struct{}

	pool  *sync.Pool
	mutex sync.Mutex

	esClient *elasticsearch.Client
}

func main() {
	opts := options{
		index:  []string{"_all"},
		fields: []string{"*"},
		scroll: 30 * time.Minute,
	}

	var indexPrefix string
	var fields string
	flag.StringVar(&opts.q, "q", "", "Query string in Lucene syntax.")
	flag.BoolVar(&opts.rawQuery, "r", false, "Switch query format in the Query DSL.")
	flag.StringVar(&u, "u", "http://localhost:9200", "Elasticsearch host URL. Default is \"http://localhost:9200\".")
	flag.StringVar(&auth, "a", "", "Elasticsearch basic authentication in the form of username:password.")
	flag.StringVar(&indexPrefix, "i", "", "Index name prefix(es). Split with ','. Default is _all.")
	flag.StringVar(&opts.docType, "d", "_doc", "Document type(s).")
	flag.StringVar(&fields, "f", "", "List of selected fields in output.")
	flag.BoolVar(&opts.print, "p", false, "Print to stdout. Default is false.")
	flag.StringVar(&opts.outputFile, "o", "./es.export.log", "Path to export file. Default is ./es.export.log.")
	flag.IntVar(&opts.maxResult, "m", 0, "Maximum number of results to return. Default is 0, No Limit.")
	flag.IntVar(&opts.size, "s", 1000, "Scroll size for each batch of results. Default is 100.")
	flag.Parse()

	opts.output = os.Stdout
	if !opts.print {
		fp, err := os.OpenFile(opts.outputFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0777)
		if err != nil {
			log.Fatalln(err)
		}
		defer fp.Close()
		opts.output = fp
	}

	opts.address = strings.Split(u, ",")
	if auth != "" && strings.Contains(auth, ":") {
		authArr := strings.Split(auth, ":")
		if len(authArr) == 2 {
			opts.username = authArr[0]
			opts.password = authArr[1]
		}
	}

	if indexPrefix != "" {
		indexArr := strings.Split(indexPrefix, ",")
		var index []string
		for _, v := range indexArr {
			if v == "" {
				continue
			}
			if v == "_all" {
				index = []string{v}
				break
			} else {
				index = append(index, v)
			}
		}
		opts.index = index
	}
	if fields != "" {
		fieldArr := strings.Split(fields, ",")
		var queryFields []string
		for _, v := range fieldArr {
			if v == "" {
				continue
			}
			queryFields = append(queryFields, v)
		}
		opts.fields = queryFields
	}

	app := &App{
		opts: opts,
		sIDs: map[string]struct{}{},
		pool: &sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer(make([]byte, 256))
			},
		},
	}

	app.run()
}

func (t *App) run() {
	t.getClient()
	t.checkIndex()
	t.buildQuery()
	t.search()
	t.clearScroll()
	log.Println("All done")
	log.Println("queryResult:", t.queryResult)
	log.Println("numResult:", t.numResult)
}

//getClient get a new elasticsearch client instance
func (t *App) getClient() *App {
	cfg := elasticsearch.Config{
		Addresses: t.opts.address,
		Username:  t.opts.username,
		Password:  t.opts.password,
	}
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}

	res, err := client.Info()
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}

	// Check response status
	if res.IsError() {
		log.Fatalf("Error: %s", res.String())
	}

	var r map[string]interface{}
	// Deserialize the response into a map.
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}

	// Print client and server version numbers.
	log.Printf("Client: %s,Server: %s", elasticsearch.Version, r["version"].(map[string]interface{})["number"])

	t.esClient = client

	return t
}

//checkIndex check if index(s) exists.
func (t *App) checkIndex() {
	if len(t.opts.index) == 0 {
		log.Fatalf("Error index")
	}

	for _, name := range t.opts.index {
		if name == "_all" {
			t.opts.index = []string{name}
			break
		}
	}

	res, err := t.esClient.Indices.Exists(t.opts.index)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}

	if res.StatusCode != 200 {
		log.Fatalf("Any of index(es) {%s} does not exist in {%s}.", strings.Join(t.opts.index, ","), strings.Join(t.opts.address, ","))
	}
}

//buildQuery format query flags to Query DSL.
func (t *App) buildQuery() *App {
	//check if queryStr is valid
	var buf bytes.Buffer
	if t.opts.rawQuery && t.opts.q != "" {
		query := map[string]interface{}{}
		err := json.Unmarshal([]byte(t.opts.q), &query)
		if err != nil {
			log.Fatalf("Unmarshal with error: %+v", err)
		}
		if err := json.NewEncoder(&buf).Encode(query); err != nil {
			log.Fatalf("Error encoding query: %s", err)
		}
		t.opts.q = ""
	}
	t.opts.queryBody = &buf

	return t
}

//result is search result.
type result struct {
	ScrollID string  `json:"_scroll_id"`
	Took     float64 `json:"took"`
	Hits     hits    `json:"hits"`
}

type hits struct {
	Total uint64 `json:"total"`
	Hits  []hit  `json:"hits"`
}

type hit struct {
	Source interface{} `json:"_source"`
}

//scroll do search scroll
func (t *App) scroll(scrollID string) {
	res, err := t.esClient.Scroll(
		t.esClient.Scroll.WithContext(context.Background()),
		t.esClient.Scroll.WithBody(t.opts.queryBody),
		t.esClient.Scroll.WithScroll(t.opts.scroll),
		t.esClient.Scroll.WithScrollID(scrollID),
		t.esClient.Scroll.WithHuman(),
	)

	defer res.Body.Close()
	if err != nil {
		log.Fatalf("Error getting scroll response: %s", err)
	}

	t.parseResult(res)
}

//search returns results matching a query.
func (t *App) search() {
	res, err := t.esClient.Search(
		t.esClient.Search.WithContext(context.Background()),
		t.esClient.Search.WithIndex(strings.Join(t.opts.index, ",")),
		t.esClient.Search.WithDocumentType(t.opts.docType),
		t.esClient.Search.WithQuery(t.opts.q),
		t.esClient.Search.WithBody(t.opts.queryBody),
		t.esClient.Search.WithSource(strings.Join(t.opts.fields, ",")),
		t.esClient.Search.WithSize(t.opts.size),
		t.esClient.Search.WithSort(strings.Join(t.opts.sort, ",")),
		t.esClient.Search.WithScroll(t.opts.scroll),
		//The actual number returned is shared count multiplied by count if elasticsearch is in cluster mode.
		t.esClient.Search.WithTerminateAfter(t.opts.maxResult),
	)

	defer res.Body.Close()
	if err != nil {
		log.Fatalf("Error getting search response: %s", err)
	}
	t.parseResult(res)
}

//parseResult parse the elasticsearch response.
func (t *App) parseResult(response *esapi.Response) {
	if response.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(response.Body).Decode(&e); err != nil {
			log.Fatalf("Error parsing the response body: %s", err)
		} else {
			// Print the response status and error information.
			log.Fatalf("[%s] %s: %s",
				response.Status(),
				e["error"].(map[string]interface{})["type"],
				e["error"].(map[string]interface{})["reason"],
			)
		}
	}

	var r result
	if err := json.NewDecoder(response.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}

	t.queryResult = r.Hits.Total
	//fix scroll with max result limit.
	if t.opts.maxResult > 0 {
		t.numResult = uint64(t.opts.maxResult)
	} else {
		t.numResult = t.queryResult
	}

	if t.numResult > 0 && len(r.Hits.Hits) > 0 {
		for totalLines != t.numResult {
			if _, ok := t.sIDs[r.ScrollID]; !ok {
				t.sIDs[r.ScrollID] = struct{}{}
			}

			for _, v := range r.Hits.Hits {
				totalLines++
				hitResult = append(hitResult, v)

				if len(hitResult) == FLUSHBUFFER {
					t.flushToFile(hitResult)
					hitResult = []hit{}
				}

				if t.opts.maxResult > 0 && int(totalLines) == t.opts.maxResult {
					t.flushToFile(hitResult)
					hitResult = []hit{}
					log.Printf("Hit max result limit: %d records", t.opts.maxResult)
					break
				}
			}

			t.scroll(r.ScrollID)
		}

		if len(hitResult) > 0 {
			t.flushToFile(hitResult)
			hitResult = []hit{}
		}
	}
}

//flushToFile flush the result to file
func (t *App) flushToFile(hits []hit) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	buf := t.pool.Get().(*bytes.Buffer)
	buf.Reset()
	defer t.pool.Put(buf)

	for _, v := range hits {
		j, _ := json.Marshal(v.Source)

		buf.Write(j)
		buf.WriteByte('\n')
	}

	t.opts.output.Write(buf.Bytes())
}

//clearScroll clears the search context for a scroll.
func (t *App) clearScroll() {
	t.esClient.ClearScroll.WithScrollID(strings.Join(t.scrollIDs, ","))
}
