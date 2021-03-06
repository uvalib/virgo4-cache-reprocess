package main

import (
	"io"
	"log"
	"os"
	"time"

	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
)

// time to wait before flushing pending records
var flushTimeout = 5 * time.Second

//
// main entry point
//
func main() {

	log.Printf("===> %s service staring up (version: %s) <===", os.Args[0], Version())

	// Get config params and use them to init service context. Any issues are fatal
	cfg := LoadConfiguration()

	// load our AWS_SQS helper object
	aws, err := awssqs.NewAwsSqs(awssqs.AwsSqsConfig{MessageBucketName: cfg.MessageBucketName})
	fatalIfError(err)

	// get the queue handles from the queue name
	inQueueHandle, err := aws.QueueHandle(cfg.InQueueName)
	fatalIfError(err)

	outQueueHandle, err := aws.QueueHandle(cfg.OutQueueName)
	fatalIfError(err)

	cacheProxy, err := NewCacheProxy(cfg)
	fatalIfError(err)

	// create the channel of inbound items
	inboundRecordsChan := make(chan Record, cfg.InboundWorkerQueueSize)

	// create the channel of inbound items
	outboundRecordsChan := make(chan awssqs.Message, cfg.OutboundWorkerQueueSize)

	// start cache workers here
	for w := 1; w <= cfg.CacheWorkers; w++ {
		go cache_worker(w, cacheProxy, inboundRecordsChan, outboundRecordsChan)
	}

	// start send workers here
	for w := 1; w <= cfg.SendWorkers; w++ {
		go send_worker(w, *cfg, aws, outQueueHandle, outboundRecordsChan)
	}

	for {
		// top of our processing loop
		err = nil

		// notification that there is one or more new ingest files to be processed
		inbound, receiptHandle, e := getInboundNotification(*cfg, aws, inQueueHandle)
		fatalIfError(e)

		// download each file and validate it
		localNames := make([]string, 0, len(inbound))
		for ix, f := range inbound {

			// download the file
			localFile, e := s3download(cfg.DownloadDir, f.SourceBucket, f.SourceKey, f.ObjectSize)
			fatalIfError(e)

			// save the local name, we will need it later
			localNames = append(localNames, localFile)

			log.Printf("Validating %s/%s (%s)", f.SourceBucket, f.SourceKey, localNames[ix])

			// create a new loader
			loader, e := NewRecordLoader(localNames[ix])
			fatalIfError(e)

			// validate the file and ensure each item appears in the cache
			e = loader.Validate(cacheProxy)
			loader.Done()
			if e == nil {
				log.Printf("INFO: %s/%s (%s) appears to be OK, ready for ingest", f.SourceBucket, f.SourceKey, localNames[ix])
			} else {
				log.Printf("ERROR: %s/%s (%s) appears to be invalid, ignoring it (%s)", f.SourceBucket, f.SourceKey, localNames[ix], e.Error())
				err = e
				break
			}
		}

		// one of the files was invalid, we need to ignore the entire batch and delete the local files
		if err != nil {
			for _, f := range localNames {
				log.Printf("INFO: removing invalid file %s", f)
				e := os.Remove(f)
				fatalIfError(e)
			}

			// go back to waiting for the next notification
			continue
		}

		// if we got here without an error then all the files are valid to be loaded... we can delete the inbound message
		// because it has been processed

		delMessages := make([]awssqs.Message, 0, 1)
		delMessages = append(delMessages, awssqs.Message{ReceiptHandle: receiptHandle})
		opStatus, err := aws.BatchMessageDelete(inQueueHandle, delMessages)
		if err != nil {
			if err != awssqs.ErrOneOrMoreOperationsUnsuccessful {
				fatalIfError(err)
			}
		}

		// check the operation results
		for ix, op := range opStatus {
			if op == false {
				log.Printf("ERROR: message %d failed to delete", ix)
			}
		}

		// now we can process each of the inbound files
		for ix, f := range inbound {

			start := time.Now()
			log.Printf("Processing %s/%s (%s)", f.SourceBucket, f.SourceKey, localNames[ix])

			loader, err := NewRecordLoader(localNames[ix])
			// fatal fail here because we have already validated the file and believe it to be correct so this
			// is some other sort of failure
			fatalIfError(err)

			// get the first record
			count := 0
			rec, err := loader.First()
			if err != nil {
				// are we done
				if err == io.EOF {
					log.Printf("WARNING: EOF on first read, looks like an empty file")
				} else {
					// fatal fail here because we have already validated the file and believe it to be correct so this
					// is some other sort of failure
					log.Fatal(err)
				}
			}

			// we can get here with an error if the first read yields EOF
			if err == nil {
				for {
					count++
					inboundRecordsChan <- rec

					rec, err = loader.Next()
					if err != nil {
						if err == io.EOF {
							// this is expected, break out of the processing loop
							break
						}
						// fatal fail here because we have already validated the file and believe it to be correct so this
						// is some other sort of failure
						log.Fatal(err)
					}
				}
			}

			loader.Done()
			duration := time.Since(start)
			log.Printf("Done processing %s/%s (%s). %d records (%0.2f tps)", f.SourceBucket, f.SourceKey, localNames[ix], count, float64(count)/duration.Seconds())

			// file has been ingested, remove it
			log.Printf("INFO: removing processed file %s", localNames[ix])
			err = os.Remove(localNames[ix])
			fatalIfError(err)
		}
	}
}

//
// end of file
//
