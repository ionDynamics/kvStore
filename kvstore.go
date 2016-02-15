package kvStore //import "go.iondynamics.net/kvStore"

type Provider interface {
	Read(bucket, key []byte, ptr interface{}) error
	Upsert(bucket, key []byte, val interface{}) error
	Delete(bucket, key []byte) error
	Exists(bucket, key []byte) (bool, error)
	All(bucket []byte, gen func() interface{}) ([]interface{}, error)
	Close() error
}

type NotFoundError interface {
	error
	IsNotFoundError()
}

func IsNotFound(err error) bool {
	_, ok := err.(NotFoundError)
	return ok
}

var dbo Provider

func Init(instance Provider) {
	dbo = instance
}

func Close() error {
	if dbo == nil {
		return NotInitializedError{}
	}
	return dbo.Close()
}

func Read(bucket, key []byte, ptr interface{}) error {
	if dbo == nil {
		return NotInitializedError{}
	}
	return dbo.Read(bucket, key, ptr)
}

func Upsert(bucket, key []byte, val interface{}) error {
	if dbo == nil {
		return NotInitializedError{}
	}
	return dbo.Upsert(bucket, key, val)
}

func Delete(bucket, key []byte) error {
	if dbo == nil {
		return NotInitializedError{}
	}
	return dbo.Delete(bucket, key)
}

func Exists(bucket, key []byte) (bool, error) {
	if dbo == nil {
		return false, NotInitializedError{}
	}
	return dbo.Exists(bucket, key)
}

func All(bucket []byte, ptrGen func() interface{}) ([]interface{}, error) {
	if dbo == nil {
		return []interface{}{}, NotInitializedError{}
	}
	return dbo.All(bucket, ptrGen)
}

type NotInitializedError struct{}

func (e NotInitializedError) Error() string {
	return "not initialized"
}
