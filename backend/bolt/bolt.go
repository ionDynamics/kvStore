package bolt //import "go.iondynamics.net/kvStore/backend/bolt"

import (
	"encoding/json"
	"time"

	"github.com/boltDb/bolt"

	"go.iondynamics.net/kvStore"
)

type NotFoundErr struct{}

func (e *NotFoundErr) Error() string {
	return "not found"
}

func (e *NotFoundErr) IsNotFoundError() {}

type Bolt struct {
	Db *bolt.DB
}

func InitBolt(path string) (kvStore.Provider, error) {
	var err error
	b := &Bolt{}
	b.Db, err = bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	return b, err
}

func (blt *Bolt) Upsert(bucket, key []byte, val interface{}) error {
	return blt.Db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}

		byt, err := json.Marshal(val)
		if err != nil {
			return err
		}

		return b.Put([]byte(key), byt)
	})
}

func (blt *Bolt) Read(bucket, key []byte, ptr interface{}) error {
	return blt.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return &NotFoundErr{}
		}

		byt := b.Get([]byte(key))
		if byt == nil {
			return &NotFoundErr{}
		}

		return json.Unmarshal(byt, ptr)
	})
}

func (blt *Bolt) Delete(bucket, key []byte) error {
	return blt.Db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}

		return b.Delete(key)
	})
}

func (blt *Bolt) Exists(bucket, key []byte) (bool, error) {
	return blt.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return &NotFoundErr{}
		}

		byt := b.Get([]byte(key))
		if byt == nil {
			return &NotFoundErr{}
		}
		return nil
	}) == nil, nil
}

func (blt *Bolt) All(bucket []byte, ptrGen func() interface{}) ([]interface{}, error) {
	var slice []interface{}
	return slice, blt.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return &NotFoundErr{}
		}

		return b.ForEach(func(k, v []byte) error {
			ptr := ptrGen()
			err := json.Unmarshal(v, ptr)
			if err != nil {
				return err
			}
			slice = append(slice, ptr)
			return nil
		})
	})
}

func (blt *Bolt) Close() error {
	return blt.Db.Close()
}
