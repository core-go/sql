package echo

import (
	"context"
	"database/sql"
	"encoding/json"
	q "github.com/core-go/sql"
	"github.com/gin-gonic/gin"
	"net/http"
	"strconv"
	"time"
)

type Handler struct {
	DB        *sql.DB
	Transform func(s string) string
	Cache     q.TxCache
	Generate  func(ctx context.Context) (string, error)
	Error     func(context.Context, string, ...map[string]interface{})
}

const d = 120 * time.Second
func NewHandler(db *sql.DB, transform func(s string) string, cache q.TxCache, generate func(context.Context) (string, error), options... func(context.Context, string, ...map[string]interface{})) *Handler {
	var logError func(context.Context, string, ...map[string]interface{})
	if len(options) >= 1 {
		logError = options[0]
	}
	return &Handler{DB: db, Transform: transform, Cache: cache, Generate: generate, Error: logError}
}
func (h *Handler) BeginTransaction(ctx *gin.Context) {
	r := ctx.Request
	id, er0 := h.Generate(r.Context())
	if er0 != nil {
		ctx.String(http.StatusInternalServerError, er0.Error())
		return
	}
	tx, er1 := h.DB.Begin()
	if er1 != nil {
		ctx.String(http.StatusInternalServerError, er1.Error())
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
	ctx.JSON(http.StatusOK, id)
}
func (h *Handler) EndTransaction(ctx *gin.Context) {
	r := ctx.Request
	ps := r.URL.Query()
	stx := ps.Get("tx")
	if len(stx) == 0 {
		ctx.String(http.StatusBadRequest, "tx is required")
		return
	}
	tx, er0 := h.Cache.Get(stx)
	if er0 != nil {
		ctx.String(http.StatusInternalServerError, er0.Error())
		return
	}
	if tx == nil {
		ctx.String(http.StatusBadRequest, "cannot get tx from cache. Maybe tx got timeout")
		return
	}
	rollback := ps.Get("rollback")
	if rollback == "true" {
		er1 := tx.Rollback()
		if er1 != nil {
			ctx.String(http.StatusInternalServerError, er1.Error())
		} else {
			h.Cache.Remove(stx)
			ctx.JSON(http.StatusOK, "true")
		}
	} else {
		er1 := tx.Commit()
		if er1 != nil {
			ctx.String(http.StatusInternalServerError, er1.Error())
		} else {
			h.Cache.Remove(stx)
			ctx.JSON(http.StatusOK, "true")
		}
	}
}
func (h *Handler) Exec(ctx *gin.Context) {
	r := ctx.Request
	s := q.JStatement{}
	er0 := json.NewDecoder(r.Body).Decode(&s)
	if er0 != nil {
		ctx.String(http.StatusBadRequest, er0.Error())
		return
	}
	s.Params = q.ParseDates(s.Params, s.Dates)
	ps := r.URL.Query()
	stx := ps.Get("tx")
	if len(stx) == 0 {
		res, er1 := h.DB.Exec(s.Query, s.Params...)
		if er1 != nil {
			handleError(ctx, 500, er1.Error(), h.Error, er1)
			return
		}
		a2, er2 := res.RowsAffected()
		if er2 != nil {
			handleError(ctx, http.StatusInternalServerError, er2.Error(), h.Error, er2)
			return
		}
		ctx.JSON(http.StatusOK, a2)
	} else {
		tx, er0 := h.Cache.Get(stx)
		if er0 != nil {
			ctx.String(http.StatusInternalServerError, er0.Error())
			return
		}
		if tx == nil {
			ctx.String(http.StatusInternalServerError, "cannot get tx from cache. Maybe tx got timeout")
			return
		}
		res, er1 := tx.Exec(s.Query, s.Params...)
		if er1 != nil {
			tx.Rollback()
			h.Cache.Remove(stx)
			handleError(ctx, 500, er1.Error(), h.Error, er1)
			return
		}
		a2, er2 := res.RowsAffected()
		if er2 != nil {
			handleError(ctx, http.StatusInternalServerError, er2.Error(), h.Error, er2)
			return
		}
		commit := ps.Get("commit")
		if commit == "true" {
			er3 := tx.Commit()
			if er3 != nil {
				handleError(ctx, http.StatusInternalServerError, er3.Error(), h.Error, er3)
				return
			}
			h.Cache.Remove(stx)
		}
		ctx.JSON(http.StatusOK, a2)
	}
}

func (h *Handler) Query(ctx *gin.Context) {
	r := ctx.Request
	s := q.JStatement{}
	er0 := json.NewDecoder(r.Body).Decode(&s)
	if er0 != nil {
		ctx.String(http.StatusBadRequest, er0.Error())
		return
	}
	s.Params = q.ParseDates(s.Params, s.Dates)
	ps := r.URL.Query()
	stx := ps.Get("tx")
	if len(stx) == 0 {
		res, er1 := q.QueryMap(r.Context(), h.DB, h.Transform, s.Query, s.Params...)
		if er1 != nil {
			handleError(ctx, 500, er1.Error(), h.Error, er1)
			return
		}
		ctx.JSON(http.StatusOK, res)
	} else {
		tx, er0 := h.Cache.Get(stx)
		if er0 != nil {
			ctx.String(http.StatusInternalServerError, er0.Error())
			return
		}
		if tx == nil {
			ctx.String(http.StatusInternalServerError, "cannot get tx from cache. Maybe tx got timeout")
			return
		}
		res, er1 := q.QueryMapWithTx(r.Context(), tx, h.Transform, s.Query, s.Params...)
		if er1 != nil {
			handleError(ctx, 500, er1.Error(), h.Error, er1)
			return
		}
		commit := ps.Get("commit")
		if commit == "true" {
			er3 := tx.Commit()
			if er3 != nil {
				handleError(ctx, http.StatusInternalServerError, er3.Error(), h.Error, er3)
				return
			}
			h.Cache.Remove(stx)
		}
		ctx.JSON(http.StatusOK, res)
	}
}
func (h *Handler) QueryOne(ctx *gin.Context) {
	r := ctx.Request
	s := q.JStatement{}
	er0 := json.NewDecoder(r.Body).Decode(&s)
	if er0 != nil {
		ctx.String(http.StatusBadRequest, er0.Error())
		return
	}
	s.Params = q.ParseDates(s.Params, s.Dates)
	ps := r.URL.Query()
	stx := ps.Get("tx")
	if len(stx) == 0 {
		res, er1 := q.QueryMap(r.Context(), h.DB, h.Transform, s.Query, s.Params...)
		if er1 != nil {
			handleError(ctx, 500, er1.Error(), h.Error, er1)
			return
		}
		if len(res) > 0 {
			ctx.JSON(http.StatusOK, res[0])
		} else {
			ctx.JSON(http.StatusOK, nil)
		}
	} else {
		tx, er0 := h.Cache.Get(stx)
		if er0 != nil {
			ctx.String(http.StatusInternalServerError, er0.Error())
			return
		}
		if tx == nil {
			ctx.String(http.StatusInternalServerError, "cannot get tx from cache. Maybe tx got timeout")
			return
		}
		res, er1 := q.QueryMapWithTx(r.Context(), tx, h.Transform, s.Query, s.Params...)
		if er1 != nil {
			handleError(ctx, 500, er1.Error(), h.Error, er1)
			return
		}
		commit := ps.Get("commit")
		if commit == "true" {
			er3 := tx.Commit()
			if er3 != nil {
				handleError(ctx, http.StatusInternalServerError, er3.Error(), h.Error, er3)
				return
			}
			h.Cache.Remove(stx)
		}
		if len(res) > 0 {
			ctx.JSON(http.StatusOK, res[0])
		} else {
			ctx.JSON(http.StatusOK, nil)
		}
	}
}
func (h *Handler) ExecBatch(ctx *gin.Context) {
	r := ctx.Request
	var s []q.JStatement
	b := make([]q.Statement, 0)
	er0 := json.NewDecoder(r.Body).Decode(&s)
	if er0 != nil {
		ctx.String(http.StatusBadRequest, er0.Error())
		return
	}
	l := len(s)
	for i := 0; i < l; i++ {
		st := q.Statement{}
		st.Query = s[i].Query
		st.Params = q.ParseDates(s[i].Params, s[i].Dates)
		b = append(b, st)
	}
	ps := r.URL.Query()
	stx := ps.Get("tx")
	var er1 error
	var res int64
	if len(stx) == 0 {
		master := ps.Get("master")
		if master == "true" {
			res, er1 = q.ExecuteBatch(r.Context(), h.DB, b, true, true)
		} else {
			res, er1 = q.ExecuteAll(r.Context(), h.DB, b...)
		}
	} else {
		tx, er0 := h.Cache.Get(stx)
		if er0 != nil {
			ctx.String(http.StatusInternalServerError, er0.Error())
			return
		}
		if tx == nil {
			ctx.String(http.StatusInternalServerError, "cannot get tx from cache. Maybe tx got timeout")
			return
		}
		tc := false
		commit := ps.Get("commit")
		if commit == "true" {
			tc = true
		}
		res, er1 = q.ExecuteStatements(r.Context(), tx, tc, b...)
		if tc && er1 == nil {
			h.Cache.Remove(stx)
		}
	}
	if er1 != nil {
		handleError(ctx, 500, er1.Error(), h.Error, er1)
		return
	}
	ctx.JSON(http.StatusOK, res)
}

func handleError(ctx *gin.Context, code int, result interface{}, logError func(context.Context, string, ...map[string]interface{}), err error) {
	if logError != nil {
		logError(ctx.Request.Context(), err.Error())
	}
	ctx.JSON(code, result)
}
