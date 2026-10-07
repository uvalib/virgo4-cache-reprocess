package main

import (
	"log"
	"time"
)

func fatalIfError(err error) {
	if err != nil {
		log.Fatalf("FATAL ERROR: %s", err.Error())
	}
}

// stop the timer, discard any pending expiry and restart it. The non-blocking drain is
// safe under both the pre and post Go 1.23 timer channel semantics
func resetTimer(t *time.Timer, d time.Duration) {
	if t.Stop() == false {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

//
// end of file
//
