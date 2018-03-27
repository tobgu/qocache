package http

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	qf "github.com/tobgu/qframe"
	"github.com/tobgu/qocache/cache"
	"github.com/tobgu/qocache/query"
	"log"
	"net/http"
	"strings"
	"time"
)

type application struct {
	cache cache.Cache
}

func trim(s string) string {
	return strings.Trim(s, " ")
}

func headerToKeyValues(headers http.Header, headerName string) (map[string]string, error) {
	keyVals := make(map[string]string)
	h := trim(headers.Get(headerName))
	if strings.HasPrefix(h, "{") {
		// Assume JSON dict
		err := json.Unmarshal([]byte(h), &keyVals)
		if err != nil {
			err = fmt.Errorf("could not JSON decode content in header %s: %s. %s", headerName, h, err.Error())
		}
		return keyVals, err
	}

	// Key-val format: key=val,key2=val2, ...
	for _, kv := range strings.Split(headers.Get(headerName), ",") {
		if kv != "" {
			kvSlice := strings.Split(kv, "=")
			if len(kvSlice) != 2 {
				return nil, fmt.Errorf("invalid key=value pair in X-QCache-keyVals: %s", kv)
			}
			keyVals[trim(kvSlice[0])] = trim(kvSlice[1])
		}
	}

	return keyVals, nil
}

func headersToCsvConfig(headers http.Header) ([]qf.CsvConfigFunc, error) {
	typs, err := headerToKeyValues(headers, "X-QCache-types")
	if err != nil {
		return nil, err
	}
	return []qf.CsvConfigFunc{qf.Types(typs)}, nil
}

func (a *application) newDataset(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	key := vars["key"]

	switch r.Header.Get("Content-Type") {
	case "text/csv":
		configFns, err := headersToCsvConfig(r.Header)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		frame := qf.ReadCsv(r.Body, configFns...)
		if frame.Err != nil {
			errorMsg := fmt.Sprintf("Could not decode CSV data: %v", frame.Err)
			http.Error(w, errorMsg, http.StatusBadRequest)
			return
		}
		a.cache.Put(key, frame, frame.ByteSize())
		w.WriteHeader(http.StatusCreated)
	case "application/json":
		frame := qf.ReadJson(r.Body)
		if frame.Err != nil {
			errorMsg := fmt.Sprintf("Could not decode JSON data: %v", frame.Err)
			http.Error(w, errorMsg, http.StatusBadRequest)
			return
		}
		a.cache.Put(key, frame, frame.ByteSize())
		w.WriteHeader(http.StatusCreated)
	default:
		http.Error(w, "Unknown content type", http.StatusBadRequest)
	}
}

func (a *application) queryDataset(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]
	item, ok := a.cache.Get(key)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	frame := item.(qf.QFrame)
	var err error = nil
	r.ParseForm()
	if qstring := r.Form.Get("q"); qstring != "" {
		frame, err = query.Query(frame, qstring)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error executing query: %s", err.Error()), http.StatusBadRequest)
			return
		}
	}

	accept := r.Header.Get("Accept")
	w.Header().Set("Content-Type", accept)

	switch accept {
	case "text/csv":
		err = frame.ToCsv(w)
	case "application/json":
		err = frame.ToJson(w, "records")
	default:
		http.Error(w, "Unknown accept type", http.StatusBadRequest)
		return
	}

	if err != nil {
		// TODO: Investigate which errors that should panic
		log.Fatalf("Failed writing JSON: %v", err)
	}
}

func Application() *mux.Router {
	// TODO make this configurable
	app := application{cache: cache.New(1000000000, 24*time.Hour)}
	r := mux.NewRouter()
	// Mount on both qcache and qocache for compatibility with qcache
	for _, root := range []string{"/qcache", "/qocache"} {
		r.HandleFunc(root+"/dataset/{key}", app.newDataset).Methods("POST")
		r.HandleFunc(root+"/dataset/{key}", app.queryDataset).Methods("GET")
	}
	return r
}
