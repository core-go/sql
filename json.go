package sql

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

type Handler struct {
	DB        *sql.DB
	Transform func(s string) string
	Master    string
	Cache     TxCache
	Generate  func(ctx context.Context) (string, error)
	Error     func(context.Context, string)
}

const d = 120 * time.Second
func NewHandlerWithTx(db *sql.DB, transform func(s string) string, master string, cache TxCache, generate func(context.Context) (string, error), options... func(context.Context, string)) *Handler {
	if len(master) == 0 {
		master = "master"
	}
	var logError func(context.Context, string)
	if len(options) >= 1 {
		logError = options[0]
	}
	return &Handler{DB: db, Transform: transform, Master: master, Cache: cache, Generate: generate, Error: logError}
}
func NewHandler(db *sql.DB, transform func(s string) string, master string, options... func(context.Context, string)) *Handler {
	return NewHandlerWithTx(db, transform, master, nil, nil, options...)
}
func (h *Handler) BeginTransaction(w http.ResponseWriter, r *http.Request) {
	id, er0 := h.Generate(r.Context())
	if er0 != nil {
		http.Error(w, er0.Error(), http.StatusInternalServerError)
		return
	}
	tx, er1 := h.DB.Begin()
	if er1 != nil {
		http.Error(w, er1.Error(), http.StatusInternalServerError)
		return
	}
	ps := r.URL.Query()
	t := d
	st := ps.Get("timeout")
	if len(st) > 0 {
		i, er2 := strconv.ParseInt(st, 10, 64)
		if er2 == nil && i > 0 {
			t = time.Duration(i) * time.Second
		}
	}
	h.Cache.Put(id, tx, t)
	succeed(w, r, http.StatusOK, id)
}
func (h *Handler) EndTransaction(w http.ResponseWriter, r *http.Request) {
	ps := r.URL.Query()
	stx := ps.Get("tx")
	if len(stx) == 0 {
		http.Error(w, "tx is required", http.StatusBadRequest)
		return
	}
	tx, er0 := h.Cache.Get(stx)
	if er0 != nil {
		http.Error(w, er0.Error(), http.StatusInternalServerError)
		return
	}
	if tx == nil {
		http.Error(w, "cannot get tx from cache. Maybe tx got timeout", http.StatusInternalServerError)
		return
	}
	rollback := ps.Get("rollback")
	if rollback == "true" {
		er1 := tx.Rollback()
		if er1 != nil {
			http.Error(w, er1.Error(), http.StatusInternalServerError)
		} else {
			h.Cache.Remove(stx)
			succeed(w, r, http.StatusOK, true)
		}
	} else {
		er1 := tx.Commit()
		if er1 != nil {
			http.Error(w, er1.Error(), http.StatusInternalServerError)
		} else {
			h.Cache.Remove(stx)
			succeed(w, r, http.StatusOK, true)
		}
	}
}
func (h *Handler) Exec(w http.ResponseWriter, r *http.Request) {
	s := JStatement{}
	er0 := json.NewDecoder(r.Body).Decode(&s)
	if er0 != nil {
		http.Error(w, er0.Error(), http.StatusBadRequest)
		return
	}
	s.Params = ParseDates(s.Params, s.Dates)
	ps := r.URL.Query()
	stx := ps.Get("tx")
	if len(stx) == 0 {
		res, er1 := h.DB.Exec(s.Query, s.Params...)
		if er1 != nil {
			handleError(w, r, 500, er1.Error(), h.Error, er1)
			return
		}
		a2, er2 := res.RowsAffected()
		if er2 != nil {
			handleError(w, r, http.StatusInternalServerError, er2.Error(), h.Error, er2)
			return
		}
		succeed(w, r, http.StatusOK, a2)
	} else {
		tx, er0 := h.Cache.Get(stx)
		if er0 != nil {
			http.Error(w, er0.Error(), http.StatusInternalServerError)
			return
		}
		if tx == nil {
			http.Error(w, "cannot get tx from cache. Maybe tx got timeout", http.StatusInternalServerError)
			return
		}
		res, er1 := tx.Exec(s.Query, s.Params...)
		if er1 != nil {
			tx.Rollback()
			h.Cache.Remove(stx)
			handleError(w, r, 500, er1.Error(), h.Error, er1)
			return
		}
		a2, er2 := res.RowsAffected()
		if er2 != nil {
			handleError(w, r, http.StatusInternalServerError, er2.Error(), h.Error, er2)
			return
		}
		commit := ps.Get("commit")
		if commit == "true" {
			er3 := tx.Commit()
			if er3 != nil {
				handleError(w, r, http.StatusInternalServerError, er3.Error(), h.Error, er3)
				return
			}
			h.Cache.Remove(stx)
		}
		succeed(w, r, http.StatusOK, a2)
	}
}

func (h *Handler) Query(w http.ResponseWriter, r *http.Request) {
	s := JStatement{}
	er0 := json.NewDecoder(r.Body).Decode(&s)
	if er0 != nil {
		http.Error(w, er0.Error(), http.StatusBadRequest)
		return
	}
	s.Params = ParseDates(s.Params, s.Dates)
	ps := r.URL.Query()
	stx := ps.Get("tx")
	if len(stx) == 0 {
		res, er1 := QueryMap(r.Context(), h.DB, h.Transform, s.Query, s.Params...)
		if er1 != nil {
			handleError(w, r, 500, er1.Error(), h.Error, er1)
			return
		}
		succeed(w, r, http.StatusOK, res)
	} else {
		tx, er0 := h.Cache.Get(stx)
		if er0 != nil {
			http.Error(w, er0.Error(), http.StatusInternalServerError)
			return
		}
		if tx == nil {
			http.Error(w, "cannot get tx from cache. Maybe tx got timeout", http.StatusInternalServerError)
			return
		}
		res, er1 := QueryMapWithTx(r.Context(), tx, h.Transform, s.Query, s.Params...)
		if er1 != nil {
			handleError(w, r, 500, er1.Error(), h.Error, er1)
			return
		}
		commit := ps.Get("commit")
		if commit == "true" {
			er3 := tx.Commit()
			if er3 != nil {
				handleError(w, r, http.StatusInternalServerError, er3.Error(), h.Error, er3)
				return
			}
			h.Cache.Remove(stx)
		}
		succeed(w, r, http.StatusOK, res)
	}
}

func (h *Handler) ExecBatch(w http.ResponseWriter, r *http.Request) {
	var s []JStatement
	b := make([]Statement, 0)
	er0 := json.NewDecoder(r.Body).Decode(&s)
	if er0 != nil {
		http.Error(w, er0.Error(), http.StatusBadRequest)
		return
	}
	l := len(s)
	for i := 0; i < l; i++ {
		st := Statement{}
		st.Query = s[i].Query
		st.Params = ParseDates(s[i].Params, s[i].Dates)
		b = append(b, st)
	}
	ps := r.URL.Query()
	stx := ps.Get("tx")
	var er1 error
	var res int64
	if len(stx) == 0 {
		master := ps.Get(h.Master)
		if master == "true" {
			res, er1 = ExecuteBatch(r.Context(), h.DB, b, true, true)
		} else {
			res, er1 = ExecuteAll(r.Context(), h.DB, b...)
		}
	} else {
		tx, er0 := h.Cache.Get(stx)
		if er0 != nil {
			http.Error(w, er0.Error(), http.StatusInternalServerError)
			return
		}
		if tx == nil {
			http.Error(w, "cannot get tx from cache. Maybe tx got timeout", http.StatusInternalServerError)
			return
		}
		tc := false
		commit := ps.Get("commit")
		if commit == "true" {
			tc = true
		}
		res, er1 = ExecuteStatements(r.Context(), tx, tc, b...)
		if tc && er1 == nil {
			h.Cache.Remove(stx)
		}
	}
	if er1 != nil {
		handleError(w, r, 500, er1.Error(), h.Error, er1)
		return
	}
	succeed(w, r, http.StatusOK, res)
}

func handleError(w http.ResponseWriter, r *http.Request, code int, result interface{}, logError func(context.Context, string), err error) {
	if logError != nil {
		logError(r.Context(), err.Error())
	}
	returnJSON(w, code, result)
}
func succeed(w http.ResponseWriter, r *http.Request, code int, result interface{}) {
	response, _ := json.Marshal(result)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
}
func returnJSON(w http.ResponseWriter, code int, result interface{}) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if result == nil {
		w.Write([]byte("null"))
		return nil
	}
	response, err := marshal(result)
	if err != nil {
		// log.Println("cannot marshal of result: " + err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return err
	}
	w.Write(response)
	return nil
}
func marshal(v interface{}) ([]byte, error) {
	b, ok1 := v.([]byte)
	if ok1 {
		return b, nil
	}
	s, ok2 := v.(string)
	if ok2 {
		return []byte(s), nil
	}
	return json.Marshal(v)
}
