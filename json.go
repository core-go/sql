package sql

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"time"
)

type JStatement struct {
	Query  string        `mapstructure:"query" json:"query,omitempty" gorm:"column:query" bson:"query,omitempty" dynamodbav:"query,omitempty" firestore:"query,omitempty"`
	Params []interface{} `mapstructure:"params" json:"params,omitempty" gorm:"column:params" bson:"params,omitempty" dynamodbav:"params,omitempty" firestore:"params,omitempty"`
	Dates  []int         `mapstructure:"dates" json:"dates,omitempty" gorm:"column:dates" bson:"dates,omitempty" dynamodbav:"dates,omitempty" firestore:"dates,omitempty"`
}

const (
	t1 = "2006-01-02T15:04:05Z"
	t2 = "2006-01-02T15:04:05-0700"
	t3 = "2006-01-02T15:04:05.0000000-0700"

	l1 = len(t1)
	l2 = len(t2)
	l3 = len(t3)
)

func ToDates(args []interface{}) []int {
	if args == nil || len(args) == 0 {
		ag2 := make([]int, 0)
		return ag2
	}
	var dates []int
	for i, arg := range args {
		if _, ok := arg.(time.Time); ok {
			dates = append(dates, i)
		}
		if _, ok := arg.(*time.Time); ok {
			dates = append(dates, i)
		}
	}
	return dates
}

func ParseDates(args []interface{}, dates []int) []interface{} {
	if args == nil || len(args) == 0 {
		ag2 := make([]interface{}, 0)
		return ag2
	}
	if dates == nil || len(dates) == 0 {
		return args
	}
	res := append([]interface{}{}, args...)
	for _, d := range dates {
		if d >= len(args) {
			break
		}
		a := args[d]
		if s, ok := a.(string); ok {
			switch len(s) {
			case l1:
				t, err := time.Parse(t1, s)
				if err == nil {
					res[d] = t
				}
			case l2:
				t, err := time.Parse(t2, s)
				if err == nil {
					res[d] = t
				}
			case l3:
				t, err := time.Parse(t3, s)
				if err == nil {
					res[d] = t
				}
			}
		}
	}
	return res
}

type Handler struct {
	DB     *sql.DB
	Master string
	Error  func(context.Context, string)
}

func NewHandler(db *sql.DB, master string, logError func(context.Context, string)) *Handler {
	if len(master) == 0 {
		master = "master"
	}
	return &Handler{DB: db, Master: master, Error: logError}
}

func (h *Handler) Exec(w http.ResponseWriter, r *http.Request) {
	s := JStatement{}
	er0 := json.NewDecoder(r.Body).Decode(&s)
	if er0 != nil {
		http.Error(w, er0.Error(), http.StatusBadRequest)
		return
	}
	s.Params = ParseDates(s.Params, s.Dates)
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
}

func (h *Handler) Query(w http.ResponseWriter, r *http.Request) {
	s := JStatement{}
	er0 := json.NewDecoder(r.Body).Decode(&s)
	if er0 != nil {
		http.Error(w, er0.Error(), http.StatusBadRequest)
		return
	}
	s.Params = ParseDates(s.Params, s.Dates)
	res, er1 := QueryMap(r.Context(), h.DB, s.Query, s.Params...)
	if er1 != nil {
		handleError(w, r, 500, er1.Error(), h.Error, er1)
		return
	}
	succeed(w, r, http.StatusOK, res)
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
	master := ps.Get(h.Master)
	var er1 error
	var res int64
	if master == "true" {
		res, er1 = ExecuteBatch(r.Context(), h.DB, b, true, true)
	} else {
		res, er1 = ExecuteAll(r.Context(), h.DB, b)
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
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
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
