package main

import (
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

   impl.redis = redis.NewClient( &redis.Options{
      DialTimeout: 30 * time.Second,
      ReadTimeout: 30 * time.Second,
      Addr:        "std-redis-staging.wbueu6.0001.use1.cache.amazonaws.com:6379",
      Password:    "",
      DB:          0,
      PoolSize:    4,
   })

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