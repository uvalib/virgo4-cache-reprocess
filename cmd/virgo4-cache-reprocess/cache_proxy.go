package main

import (
   "fmt"
   "github.com/go-redis/redis/v7"
   "github.com/uvalib/virgo4-sqs-sdk/awssqs"
   "time"

   //"log"
)

type CacheProxy interface {
   Exists(string) ( bool, error )
   Get(string) ( * awssqs.Message, error )
}

// our implementation
type cacheProxyImpl struct {
   redis * redis.Client
}

//
// factory
//
func NewCacheProxy( config * ServiceConfig ) ( CacheProxy, error ) {

   impl := &cacheProxyImpl{}

   options := &redis.Options{
      DialTimeout: time.Duration( config.RedisTimeout ) * time.Second,
      ReadTimeout: time.Duration( config.RedisTimeout ) * time.Second,
      Addr:        fmt.Sprintf( "%s:%d", config.RedisHost, config.RedisPort ),
      Password:    config.RedisPass,
      DB:          config.RedisDB,
      PoolSize:    4,
   }
   impl.redis = redis.NewClient( options )

      _, err := impl.redis.Ping().Result()
   return impl, err
}

//
// does the supplied id exist in the cache
//
func (ci *cacheProxyImpl) Exists(key string) ( bool, error ) {

   return true, nil
}

//
// get the specified item from the cache
//
func (ci *cacheProxyImpl) Get(key string) ( * awssqs.Message, error ) {

   // lookup the id in the cache
   //_, found := ci.c.Get(id)
   //if found {
   //log.Printf( "ID [%s] found", id )
   //   return true
   //}

   return nil, nil
}

//
// end of file
//