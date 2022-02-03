package main

import (
	"log"
	"os"
	"strconv"
)

// ServiceConfig defines all of the service configuration parameters
type ServiceConfig struct {
	InQueueName  string // SQS queue name for inbound documents
	OutQueueName string // SQS queue name for outbound documents
	PollTimeOut  int64  // the SQS queue timeout (in seconds)

	DataSourceNames   string // the data sources to include in the query
	MessageBucketName string // the bucket to use for large messages
	DownloadDir       string // the S3 file download directory (local)

	PostgresHost     string // the postgres endpoint
	PostgresPort     int    // and port
	PostgresUser     string // username
	PostgresPass     string // and password
	PostgresDatabase string // which database to use
	PostgresTable    string // which table to use

	InboundWorkerQueueSize  int // the message queue size that feeds the cache workers
	CacheWorkers            int // the number of cache worker processes
	OutboundWorkerQueueSize int // the message queue size that feeds the send workers
	SendWorkers             int // the number of send worker processes
}

func ensureSet(env string) string {
	val, set := os.LookupEnv(env)

	if set == false {
		log.Printf("FATAL ERROR: environment variable not set: [%s]", env)
		os.Exit(1)
	}

	return val
}

func ensureSetAndNonEmpty(env string) string {
	val := ensureSet(env)

	if val == "" {
		log.Printf("FATAL ERROR: environment variable not set: [%s]", env)
		os.Exit(1)
	}

	return val
}

func envToInt(env string) int {

	number := ensureSetAndNonEmpty(env)
	n, err := strconv.Atoi(number)
	fatalIfError(err)
	return n
}

// LoadConfiguration will load the service configuration from env/cmdline
// and return a pointer to it. Any failures are fatal.
func LoadConfiguration() *ServiceConfig {

	var cfg ServiceConfig

	cfg.InQueueName = ensureSetAndNonEmpty("VIRGO4_CACHE_REPROCESS_IN_QUEUE")
	cfg.OutQueueName = ensureSetAndNonEmpty("VIRGO4_CACHE_REPROCESS_OUT_QUEUE")
	cfg.PollTimeOut = int64(envToInt("VIRGO4_CACHE_REPROCESS_QUEUE_POLL_TIMEOUT"))
	cfg.DataSourceNames = ensureSetAndNonEmpty("VIRGO4_CACHE_REPROCESS_DATA_SOURCE")
	cfg.MessageBucketName = ensureSetAndNonEmpty("VIRGO4_SQS_MESSAGE_BUCKET")
	cfg.DownloadDir = ensureSetAndNonEmpty("VIRGO4_CACHE_REPROCESS_DOWNLOAD_DIR")
	cfg.PostgresHost = ensureSetAndNonEmpty("VIRGO4_CACHE_REPROCESS_POSTGRES_HOST")
	cfg.PostgresPort = envToInt("VIRGO4_CACHE_REPROCESS_POSTGRES_PORT")
	cfg.PostgresUser = ensureSetAndNonEmpty("VIRGO4_CACHE_REPROCESS_POSTGRES_USER")
	cfg.PostgresPass = ensureSetAndNonEmpty("VIRGO4_CACHE_REPROCESS_POSTGRES_PASS")
	cfg.PostgresDatabase = ensureSetAndNonEmpty("VIRGO4_CACHE_REPROCESS_POSTGRES_DATABASE")
	cfg.PostgresTable = ensureSetAndNonEmpty("VIRGO4_CACHE_REPROCESS_POSTGRES_TABLE")
	cfg.InboundWorkerQueueSize = envToInt("VIRGO4_CACHE_REPROCESS_INBOUND_WORK_QUEUE_SIZE")
	cfg.CacheWorkers = envToInt("VIRGO4_CACHE_REPROCESS_CACHE_WORKERS")
	cfg.OutboundWorkerQueueSize = envToInt("VIRGO4_CACHE_REPROCESS_OUTBOUND_WORK_QUEUE_SIZE")
	cfg.SendWorkers = envToInt("VIRGO4_CACHE_REPROCESS_SEND_WORKERS")

	log.Printf("[CONFIG] InQueueName             = [%s]", cfg.InQueueName)
	log.Printf("[CONFIG] OutQueueName            = [%s]", cfg.OutQueueName)
	log.Printf("[CONFIG] PollTimeOut             = [%d]", cfg.PollTimeOut)
	log.Printf("[CONFIG] DataSourceNames         = [%s]", cfg.DataSourceNames)
	log.Printf("[CONFIG] MessageBucketName       = [%s]", cfg.MessageBucketName)
	log.Printf("[CONFIG] DownloadDir             = [%s]", cfg.DownloadDir)
	log.Printf("[CONFIG] PostgresHost            = [%s]", cfg.PostgresHost)
	log.Printf("[CONFIG] PostgresPort            = [%d]", cfg.PostgresPort)
	log.Printf("[CONFIG] PostgresUser            = [%s]", cfg.PostgresUser)
	log.Printf("[CONFIG] PostgresPass            = [REDACTED]")
	log.Printf("[CONFIG] PostgresDatabase        = [%s]", cfg.PostgresDatabase)
	log.Printf("[CONFIG] PostgresTable           = [%s]", cfg.PostgresTable)
	log.Printf("[CONFIG] InboundWorkerQueueSize  = [%d]", cfg.InboundWorkerQueueSize)
	log.Printf("[CONFIG] CacheWorkers            = [%d]", cfg.CacheWorkers)
	log.Printf("[CONFIG] OutboundWorkerQueueSize = [%d]", cfg.OutboundWorkerQueueSize)
	log.Printf("[CONFIG] SendWorkers             = [%d]", cfg.SendWorkers)

	return &cfg
}
