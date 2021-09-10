package sql

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"time"
)

type JStatement struct {
	Query string        `mapstructure:"query" json:"query,omitempty" gorm:"column:query" bson:"query,omitempty" dynamodbav:"query,omitempty" firestore:"query,omitempty"`
	Args  []interface{} `mapstructure:"args" json:"args,omitempty" gorm:"column:args" bson:"args,omitempty" dynamodbav:"args,omitempty" firestore:"args,omitempty"`
	Dates []int         `mapstructure:"dates" json:"dates,omitempty" gorm:"column:dates" bson:"dates,omitempty" dynamodbav:"dates,omitempty" firestore:"dates,omitempty"`
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
	DB *sql.DB
	Error func(context.Context, string)
}

func NewHandler(db *sql.DB, logError func(context.Context, string)) *Handler {
	return &Handler{db, logError}
}

func (h *Handler) Exec(w http.ResponseWriter, r *http.Request) {
	s := JStatement{}
	er0 := json.NewDecoder(r.Body).Decode(&s)
	if er0 != nil {
		http.Error(w, er0.Error(), http.StatusBadRequest)
		return
	}
	s.Args = ParseDates(s.Args, s.Dates)
	res, er1 := h.DB.Exec(s.Query, s.Args...)
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
		st.Args = ParseDates(s[i].Args, s[i].Dates)
		b = append(b, st)
	}

	res, er1 := ExecuteAll(r.Context(), h.DB, b)
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
