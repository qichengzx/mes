mes
---

mes is a cli tool to export elasticsearch data into JSON files.

### Installation

```go
$ go get github.com/qichengzx/mes
```

### Usage

```shell
$ mes [-h] [-u URL] [-a AUTH] [-i INDEX]
           [-d DOC_TYPE] [-f FIELD]
           [-q QUERY] [-r] [-p] [-o FILE]
           [-m INTEGER] [-s INTEGER] 

  -u, URL                    Elasticsearch host URL. Default is 'http://localhost:9200'.
  -a, AUTH                   Elasticsearch basic authentication in the form of username:password.
  -i, INDEX                  Index name prefix(es). Default is ['_all'].
  -d, DOC_TYPE               Document type(s).
  -f, FIELDS                 List of selected fields in output. Default is ['*'].
  -q, QUERY                  Query string in Lucene syntax.
  -r, Raw Query              Switch query format in the Query DSL.
  -o, FILE                   Exported file location. Default is './es.export.log'.
  -p, Print to stdout        Print query result to stdout.
  -m, INTEGER                Maximum number of results to return. Default is 0. No Limit.
  -s, INTEGER                Scroll size for each batch of results. Default is 1000.
```

