package http

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	qf "github.com/tobgu/qframe"
	"github.com/tobgu/qframe/config/csv"
	"github.com/tobgu/qframe/config/newqf"
	"github.com/tobgu/qframe/types"
	"github.com/tobgu/qocache/cache"
	"github.com/tobgu/qocache/config"
	"github.com/tobgu/qocache/qlog"
	"github.com/tobgu/qocache/query"
	"github.com/tobgu/qocache/statistics"
	qostrings "github.com/tobgu/qocache/strings"
	"io/ioutil"
	"net/http"
	"net/http/pprof"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	contentTypeJson = "application/json"
	contentTypeCsv  = "text/csv"
)

type application struct {
	cache  cache.Cache
	stats  *statistics.Statistics
	logger qlog.Logger
}

var charsetRegex = regexp.MustCompile("charset=([A-Za-z0-9_-]+)")

func trim(s string) string {
	return strings.Trim(s, " ")
}

func headerToKeyValues(headers http.Header, headerName string) (map[string]interface{}, error) {
	keyVals := make(map[string]interface{})
	h := trim(headers.Get(headerName))
	if strings.HasPrefix(h, "{") {
		// Assume JSON dict
		d := json.NewDecoder(strings.NewReader(h))
		d.UseNumber()
		err := d.Decode(&keyVals)
		if err != nil {
			err = fmt.Errorf("could not JSON decode content in header %s: %s. %s", headerName, h, err.Error())
		}

		for k, v := range keyVals {
			if t, ok := v.(json.Number); ok {
				if i, err := t.Int64(); err == nil {
					keyVals[k] = int(i)
				} else if f, err := t.Float64(); err == nil {
					keyVals[k] = f
				} else {
					keyVals[k] = t.String()
				}
			}
		}

		return keyVals, err
	}

	// Key-val format: key=val,key2=val2, ...
	for _, kv := range strings.Split(headers.Get(headerName), ";") {
		if kv != "" {
			kvSlice := strings.Split(kv, "=")
			if len(kvSlice) != 2 {
				return nil, fmt.Errorf("invalid key=value pair in X-QCache-keyVals: %s", kv)
			}
			keyVals[trim(kvSlice[0])] = trim(kvSlice[1])
		}
	}

	for k, v := range keyVals {
		s := v.(string)
		if i, err := strconv.Atoi(s); err == nil {
			keyVals[k] = i
		} else if f, err := strconv.ParseFloat(s, 64); err == nil {
			keyVals[k] = f
		} // else: No need to do anything
	}

	return keyVals, nil
}

func strIfToStrStr(m map[string]interface{}, err error) (map[string]string, error) {
	if err != nil {
		return nil, err
	}

	result := make(map[string]string, len(m))
	for k, v := range m {
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("%v is not a valid string", v)
		}
		result[k] = s
	}

	return result, nil
}

func readEnumSpec(headers http.Header) (map[string][]string, error) {
	enumSpecJson := headers.Get("X-QCache-enum-specs")
	if enumSpecJson == "" {
		return nil, nil
	}

	result := map[string][]string{}
	if err := json.Unmarshal([]byte(enumSpecJson), &result); err != nil {
		return nil, fmt.Errorf("could not decode JSON content in X-QCache-enum-specs: %s", err.Error())
	}

	return result, nil
}

func headersToCsvConfig(headers http.Header) ([]csv.ConfigFunc, error) {
	typs, err := strIfToStrStr(headerToKeyValues(headers, "X-QCache-types"))
	if err != nil {
		return nil, err
	}

	enumVals, err := readEnumSpec(headers)
	if err != nil {
		return nil, err
	}

	rowCountHint := 0
	if rowCountHintStr := headers.Get("X-QCache-row-count-hint"); rowCountHintStr != "" {
		rowCountHint, err = strconv.Atoi(rowCountHintStr)
		if err != nil {
			return nil, err
		}
	}

	return []csv.ConfigFunc{csv.Types(typs), csv.EnumValues(enumVals), csv.EmptyNull(true), csv.RowCountHint(rowCountHint)}, nil
}

func headersToJsonConfig(headers http.Header) ([]newqf.ConfigFunc, error) {
	enumVals, err := readEnumSpec(headers)
	if err != nil {
		return nil, err
	}

	return []newqf.ConfigFunc{newqf.Enums(enumVals)}, nil
}

func firstErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}

	return nil
}

func parseContentType(h string) (string, string) {
	result := strings.Split(h, ";")
	if len(result) == 1 {
		return strings.TrimSpace(h), ""
	}

	if len(result) >= 2 {
		contentType, charset := strings.TrimSpace(result[0]), ""
		match := charsetRegex.FindStringSubmatch(result[1])
		if len(match) > 1 {
			charset = match[1]
		}
		return contentType, charset
	}

	return "", ""
}

func (a *application) log(msg string, params ...interface{}) string {
	result := fmt.Sprintf(msg, params...)
	a.logger.Printf(result)
	return result
}

func (a *application) logError(source string, err error) {
	if err != nil {
		a.logger.Printf("Error %s: %v", source, err)
	}
}

func (a *application) badRequest(w http.ResponseWriter, msg string, params ...interface{}) {
	http.Error(w, a.log(msg, params...), http.StatusBadRequest)
}

func (a *application) newDataset(w http.ResponseWriter, r *http.Request) {
	statsProbe := statistics.NewStoreProbe(r.Context())
	defer r.Body.Close()
	vars := mux.Vars(r)
	key := vars["key"]

	var frame qf.QFrame
	contentType, charset := parseContentType(r.Header.Get("Content-Type"))
	if charset != "" && charset != "utf-8" {
		a.badRequest(w, "Unsupported charset: %s", charset)
		return
	}

	switch contentType {
	case contentTypeCsv:
		configFns, err := headersToCsvConfig(r.Header)
		if err != nil {
			a.badRequest(w, err.Error())
			return
		}

		frame = qf.ReadCSV(r.Body, configFns...)
	case contentTypeJson:
		configFns, err := headersToJsonConfig(r.Header)
		if err != nil {
			a.badRequest(w, err.Error())
			return
		}

		frame = qf.ReadJSON(r.Body, configFns...)
	default:
		a.badRequest(w, "Unknown content type: %s", contentType)
		return
	}

	if frame.Err != nil {
		a.badRequest(w, "Could not decode data: %v", frame.Err)
		return
	}

	frame, _, err := addStandInColumns(frame, r.Header)
	if err = firstErr(err, frame.Err); err != nil {
		a.badRequest(w, err.Error())
		return
	}

	err = a.cache.Put(key, frame, frame.ByteSize())
	a.logError("Put new dataset in cache", err)
	w.WriteHeader(http.StatusCreated)
	statsProbe.Success(frame.Len())
}

func addStandInColumns(frame qf.QFrame, headers http.Header) (qf.QFrame, bool, error) {
	standIns, err := headerToKeyValues(headers, "X-QCache-stand-in-columns")
	if err != nil {
		return frame, false, err
	}

	columnAdded := false
	for col, standIn := range standIns {
		if !frame.Contains(col) {
			if s, ok := standIn.(string); ok {
				if qostrings.IsQuoted(s) {
					// String constant
					standIn = qostrings.TrimQuotes(s)
				} else {
					// Column reference
					standIn = types.ColumnName(s)
				}
			}

			frame = frame.Eval(col, qf.Val(standIn))
			columnAdded = true
		}
	}

	return frame, columnAdded, nil
}

func formatContentType(ct string) string {
	return fmt.Sprintf("%s; charset=utf-8", ct)
}

func (a *application) queryDatasetGet(w http.ResponseWriter, r *http.Request) {
	// The query is located in the URL
	a.queryDataset(w, r, func(r *http.Request) (string, error) {
		err := r.ParseForm()
		a.logError("Form parse query", err)
		return r.Form.Get("q"), nil
	})
}

func (a *application) queryDatasetPost(w http.ResponseWriter, r *http.Request) {
	// The query is located in the body
	a.queryDataset(w, r, func(r *http.Request) (string, error) {
		defer r.Body.Close()
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return "", err
		}
		return string(b), nil
	})
}

func (a *application) queryDataset(w http.ResponseWriter, r *http.Request, qFn func(r *http.Request) (string, error)) {
	statsProbe := statistics.NewQueryProbe(r.Context())
	vars := mux.Vars(r)
	key := vars["key"]
	item, ok := a.cache.Get(key)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_, err := w.Write([]byte(fmt.Sprintf("Dataset '%s' not found", key)))
		a.logError("Query dataset write not found", err)
		statsProbe.Missing()
		return
	}
	frame := item.(qf.QFrame)
	qstring, err := qFn(r)
	if err != nil {
		a.badRequest(w, "Error reading query: %s", err.Error())
		return
	}

	frame, columnAdded, err := addStandInColumns(frame, r.Header)
	if err != nil {
		a.badRequest(w, "Error adding standin columns: %s", err.Error())
		return
	}

	if columnAdded {
		// Need to replace existing frame in cache since the new one contains
		// additional columns.
		err := a.cache.Put(key, frame, frame.ByteSize())
		a.logError("Column added put dataset in cache", err)
	}

	if qstring != "" {
		result := query.Query(frame, qstring)
		if result.Err != nil {
			a.badRequest(w, "Error executing query: %s", result.Err.Error())
			return
		}
		frame = result.Qframe
		w.Header().Set("X-QCache-unsliced-length", fmt.Sprintf("%d", result.UnslicedLen))
	}

	// This is a bit simplistic since we assume that only one content type
	// is listed and not a prioritized . Good enough for now.
	accept := r.Header.Get("Accept")
	w.Header().Set("Content-Type", formatContentType(accept))

	switch accept {
	case contentTypeCsv:
		err = frame.ToCSV(w)
	case contentTypeJson:
		err = frame.ToJSON(w)
	default:
		a.badRequest(w, "Unknown accept type: %s", accept)
		return
	}

	if err != nil {
		// Panic for now, will be picked up by recover middleware
		panic(fmt.Sprintf("Failed writing query response: %v", err))
	}

	statsProbe.Success()
}

func (a *application) statistics(w http.ResponseWriter, r *http.Request) {
	accept := r.Header.Get("Accept")
	if accept == "" || accept == "*/*" {
		accept = contentTypeJson
	}

	if accept != contentTypeJson {
		a.badRequest(w, "Unknown accept type: %s, statistics only available in JSON format", accept)
		return
	}

	w.Header().Set("Content-Type", formatContentType(accept))
	stats := a.stats.Stats()
	enc := json.NewEncoder(w)
	err := enc.Encode(stats)
	a.logError("Encoding stats", err)
}

func (a *application) status(w http.ResponseWriter, r *http.Request) {
	_, err := w.Write([]byte("OK"))
	a.logError("Write status", err)
}

func attachProfiler(router *mux.Router) {
	router.HandleFunc("/debug/pprof/", pprof.Index)
	router.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	router.HandleFunc("/debug/pprof/profile", pprof.Profile)
	router.HandleFunc("/debug/pprof/symbol", pprof.Symbol)

	router.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	router.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	router.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
	router.Handle("/debug/pprof/block", pprof.Handler("block"))
}

func Application(conf config.Config, logger qlog.Logger) (*mux.Router, error) {
	c := cache.New(conf.Size, time.Duration(conf.Age)*time.Second)
	s := statistics.New(c, conf.StatisticsBufferSize)
	app := &application{cache: c, stats: s, logger: logger}
	r := mux.NewRouter()

	middleWares := make([]middleware, 0)
	middleWares = append(middleWares, withRecover(logger))
	middleWares = append(middleWares, withStatistics(s))

	if conf.RequestLog {
		middleWares = append(middleWares, withRequestLog(app))
	}

	if conf.BasicAuth != "" {
		user, password, err := parseBasicAuth(conf.BasicAuth)
		if err != nil {
			return nil, err
		}
		middleWares = append(middleWares, withBasicAuth(app.logger, user, password))
	}

	middleWares = append(middleWares, withLz4(app))

	mw := chainMiddleware(middleWares...)

	// Mount on both qcache and qocache for compatibility with qcache
	for _, root := range []string{"/qcache", "/qocache"} {
		r.HandleFunc(root+"/dataset/{key}", mw(app.newDataset)).Methods("POST")
		r.HandleFunc(root+"/dataset/{key}/q", mw(app.queryDatasetPost)).Methods("POST")
		r.HandleFunc(root+"/dataset/{key}", mw(app.queryDatasetGet)).Methods("GET")
		r.HandleFunc(root+"/statistics", mw(app.statistics)).Methods("GET")
		r.HandleFunc(root+"/status", mw(app.status)).Methods("GET")
	}

	if conf.HttpPprof {
		attachProfiler(r)
	}

	return r, nil
}

func parseBasicAuth(auth string) (string, string, error) {
	parts := strings.Split(auth, ":")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid basic auth string, expected <username>:<password>, was: %s", auth)
	}

	username := parts[0]
	password := parts[1]
	if len(username) < 1 {
		return "", "", fmt.Errorf("invalid basic auth string, username must not be empty")
	}

	if len(password) < 1 {
		return "", "", fmt.Errorf("invalid basic auth string, password must not be empty")
	}

	return username, password, nil
}
