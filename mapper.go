package orm

import "context"

type Mapper interface {
	DbToModel(ctx context.Context, model interface{}) (interface{}, error)
	DbToModels(ctx context.Context, model interface{}) (interface{}, error)

	ModelToDb(ctx context.Context, model interface{}) (interface{}, error)
	ModelsToDb(ctx context.Context, model interface{}) (interface{}, error)
}
