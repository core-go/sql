package orm

import (
	"context"
	"gorm.io/gorm"
)

type FieldLoader struct {
	Database  *gorm.DB
	TableName string
	Name      string
}

func NewFieldLoader(db *gorm.DB, tableName string, name string) *FieldLoader {
	return &FieldLoader{
		Database:  db,
		TableName: tableName,
		Name:      name,
	}
}

func (l *FieldLoader) Values(ctx context.Context, ids []string) ([]string, error) {
	var nilArr []string
	var urlIdArr []string
	err := l.Database.Table(l.TableName).Set("gorm:auto_preload", true).Where(l.Name+" IN (?)", ids).Pluck(l.Name, &urlIdArr).Error
	if err != nil {
		return nilArr, err
	}
	return urlIdArr, nil
}
