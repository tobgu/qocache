package http

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	qf "github.com/tobgu/qframe"
	"github.com/tobgu/qframe/filter"
	"github.com/tobgu/qocache/cache"
	"github.com/tobgu/qocache/query"
	qostrings "github.com/tobgu/qocache/strings"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type application struct {
	cache cache.Cache
}

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
		} else {
			// No need to do anything
		}
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

func headersToCsvConfig(headers http.Header) ([]qf.CsvConfigFunc, error) {
	typs, err := strIfToStrStr(headerToKeyValues(headers, "X-QCache-types"))
	if err != nil {
		return nil, err
	}

	enumVals, err := readEnumSpec(headers)
	if err != nil {
		return nil, err
	}

	return []qf.CsvConfigFunc{qf.Types(typs), qf.EnumValues(enumVals)}, nil
}

func headersToJsonConfig(headers http.Header) ([]qf.ConfigFunc, error) {
	enumVals, err := readEnumSpec(headers)
	if err != nil {
		return nil, err
	}

	return []qf.ConfigFunc{qf.Enums(enumVals)}, nil
}

func firstErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *application) newDataset(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	key := vars["key"]

	var frame qf.QFrame
	switch r.Header.Get("Content-Type") {
	case "text/csv":
		configFns, err := headersToCsvConfig(r.Header)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		frame = qf.ReadCsv(r.Body, configFns...)
	case "application/json":
		configFns, err := headersToJsonConfig(r.Header)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		frame = qf.ReadJson(r.Body, configFns...)
	default:
		http.Error(w, "Unknown content type", http.StatusBadRequest)
		return
	}

	if frame.Err != nil {
		errorMsg := fmt.Sprintf("Could not decode data: %v", frame.Err)
		http.Error(w, errorMsg, http.StatusBadRequest)
		return
	}

	frame, err := addStandInColumns(frame, r.Header)
	if err = firstErr(err, frame.Err); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	a.cache.Put(key, frame, frame.ByteSize())
	w.WriteHeader(http.StatusCreated)
}

func addStandInColumns(frame qf.QFrame, headers http.Header) (qf.QFrame, error) {
	standIns, err := headerToKeyValues(headers, "X-QCache-stand-in-columns")
	if err != nil {
		return frame, err
	}

	for col, standIn := range standIns {
		if !frame.Contains(col) {
			if s, ok := standIn.(string); ok {
				if qostrings.IsQuoted(s) {
					// String constant
					standIn = qostrings.TrimQuotes(s)
				} else {
					// Column reference
					standIn = filter.ColumnName(s)
				}
			}

			frame = frame.Apply(qf.Instruction{Fn: standIn, DstCol: col})
		}
	}

	return frame, nil
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
		result := query.Query(frame, qstring)
		if result.Err != nil {
			http.Error(w, fmt.Sprintf("Error executing query: %s", result.Err.Error()), http.StatusBadRequest)
			return
		}
		frame = result.Qframe
		w.Header().Set("X-QCache-unsliced-length", fmt.Sprintf("%d", result.UnslicedLen))
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
