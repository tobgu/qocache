package http

import (
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

func headersToCsvConfig(headers http.Header) ([]qf.CsvConfigFunc, error) {
	types := make(map[string]string)
	for _, kv := range strings.Split(headers.Get("X-QCache-types"), ",") {
		if kv != "" {
			kvSlice := strings.Split(kv, "=")
			if len(kvSlice) != 2 {
				return nil, fmt.Errorf("invalid key=value pair in X-QCache-types: %s", kv)
			}
			types[trim(kvSlice[0])] = trim(kvSlice[1])
		}
	}

	println("!!! Types: ", fmt.Sprintf("%v, %v, %v", types, headers["X-QCache-types"], headers))
	return []qf.CsvConfigFunc{qf.Types(types)}, nil
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
