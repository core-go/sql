package sql

import (
	"context"
	"fmt"
	"github.com/jinzhu/gorm"
	"reflect"
	"strings"
)

type GenericService struct {
	*ViewService
	versionField   string
	versionIndex   int
	versionDBField string
}

func NewGenericService(db *gorm.DB, modelType reflect.Type, tableName string, versionField string, mapper Mapper) *GenericService {
	defaultViewService := NewViewService(db, modelType, tableName, mapper)
	if len(versionField) > 0 {
		index := FindFieldIndex(modelType, versionField)
		if index >= 0 {
			dbFieldName, exist := GetColumnNameByIndex(modelType, index)
			if !exist {
				dbFieldName = strings.ToLower(versionField)
			}
			return &GenericService{ViewService: defaultViewService, versionField: versionField, versionIndex: index, versionDBField: dbFieldName}
		}
	}
	return &GenericService{defaultViewService, versionField, -1, ""}
}

func NewDefaultGenericService(db *gorm.DB, modelType reflect.Type, tableName string) *GenericService {
	return NewGenericService(db, modelType, tableName, "", nil)
}

func (s *GenericService) Insert(ctx context.Context, model interface{}) (int64, error) {
	if s.Mapper != nil {
		m2, err := s.Mapper.ModelToDb(ctx, model)
		if err != nil {
			return 0, err
		}
		if s.versionIndex >= 0 {
			return InsertWithVersion(s.Database, s.table, m2, s.versionIndex)
		}
		return Insert(s.Database, s.table, m2)
	}
	if s.versionIndex >= 0 {
		return InsertWithVersion(s.Database, s.table, model, s.versionIndex)
	}
	return Insert(s.Database, s.table, model)
}

func (s *GenericService) Update(ctx context.Context, model interface{}) (int64, error) {
	//if status := s.Database.Table(s.table).Save(model); status.Error != nil {
	//	return 0, status.Error
	//}
	//return 1, nil
	if s.Mapper != nil {
		m2, err := s.Mapper.ModelToDb(ctx, model)
		if err != nil {
			return 0, err
		}
		model = m2
	}
	idQuery := BuildQueryByIdFromObject(model, s.modelType, s.keys)
	if len(s.versionField) > 0 {
		versionQuery := s.buildVersionQueryAndModifyModel(idQuery, model, s.versionField, s.versionDBField)
		rowAffect, err := Update(s.Database, s.table, model, versionQuery)
		if rowAffect == 0 {
			newModel := reflect.New(s.modelType).Interface()
			if isExist, _ := Exists(s.Database, s.table, newModel, idQuery); isExist {
				return -1, fmt.Errorf("wrong version")
			} else {
				return 0, fmt.Errorf("item not found")
			}
		} else {
			return rowAffect, err
		}
	}
	return Update(s.Database, s.table, model, idQuery)
}

func (s *GenericService) Patch(ctx context.Context, model map[string]interface{}) (int64, error) {
	if s.Mapper != nil {
		_, err := s.Mapper.ModelToDb(ctx, model)
		if err != nil {
			return 0, err
		}
	}
	idQuery := BuildQueryByIdFromMap(model, s.modelType, s.keys)
	if len(s.versionField) > 0 {
		versionQuery := s.buildVersionQueryAndModifyModel(idQuery, model, s.versionDBField, s.versionDBField)
		rowAffect, err := Patch(s.Database, s.table, MapToGORM(model, s.modelType), versionQuery)
		if rowAffect == 0 {
			newModel := reflect.New(s.modelType).Interface()
			if isExist, _ := Exists(s.Database, s.table, newModel, idQuery); isExist {
				return -1, fmt.Errorf("wrong version")
			} else {
				return 0, fmt.Errorf("item not found")
			}
		} else {
			return rowAffect, err
		}
	}
	return Patch(s.Database, s.table, MapToGORM(model, s.modelType), idQuery)
}

func (s *GenericService) Save(ctx context.Context, model interface{}) (int64, error) {
	if s.Mapper != nil {
		m2, err := s.Mapper.ModelToDb(ctx, model)
		if err != nil {
			return 0, err
		}
		model = m2
	}
	return Save(s.Database, s.table, model)
}

func (s *GenericService) Delete(ctx context.Context, id interface{}) (int64, error) {
	l := len(s.keys)
	if l == 1 {
		return Delete(s.Database, s.table, s.initSingleResult(), BuildQueryById(id, s.modelType, s.keys[0]))
	} else {
		ids := id.(map[string]interface{})
		return Delete(s.Database, s.table, s.initSingleResult(), MapToGORM(ids, s.modelType))
	}
}

func (s *GenericService) buildVersionQueryAndModifyModel(query map[string]interface{}, model interface{}, versionField string, versionDBField string) map[string]interface{} {
	newMap := copyMap(query)
	if v, ok := model.(map[string]interface{}); ok {
		if currentVersion, exist := v[s.versionField]; exist {
			newMap[versionDBField] = currentVersion
			switch versionValue := currentVersion.(type) {
			case int:
				{
					v[s.versionField] = versionValue + 1
				}
			default:
				panic("not support type's version")
			}
		}
	} else {
		valueOfModel := reflect.Indirect(reflect.ValueOf(model))
		valueOfCurrentVersion := valueOfModel.FieldByName(s.versionField)
		newMap[versionDBField] = valueOfCurrentVersion.Interface()
		switch valueOfCurrentVersion.Kind().String() {
		case "int":
			{
				nextVersion := reflect.ValueOf(valueOfCurrentVersion.Interface().(int) + 1)
				valueOfModel.FieldByName(s.versionField).Set(nextVersion)
			}
		default:
			panic("not support type's version")
		}
	}
	return newMap
}

func copyMap(originalMap map[string]interface{}) map[string]interface{} {
	newMap := make(map[string]interface{})
	for k, v := range originalMap {
		newMap[k] = v
	}
	return newMap
}
