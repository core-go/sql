package orm

import "gorm.io/gorm"

type UnitOfWork struct {
	DB          *gorm.DB
	Transaction *gorm.DB
}

func NewUnitOfWork(db *gorm.DB) *UnitOfWork {
	return &UnitOfWork{DB: db}
}

func (unitOfWork *UnitOfWork) BeginTransaction() *gorm.DB {
	unitOfWork.Transaction = unitOfWork.DB.Begin()
	return unitOfWork.Transaction
}

func (unitOfWork *UnitOfWork) Commit() {
	unitOfWork.Transaction.Commit()
}
