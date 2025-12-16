package main

import (
	"ClashKingTrackingGo/tracking"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load()

	logMemEvery := func(d time.Duration) {
		go func() {
			var m runtime.MemStats
			for range time.NewTicker(d).C {
				runtime.ReadMemStats(&m)
				log.Printf(
					"heap_alloc=%.2fMB heap_inuse=%.2fMB sys=%.2fMB num_gc=%d",
					float64(m.HeapAlloc)/1024/1024,
					float64(m.HeapInuse)/1024/1024,
					float64(m.Sys)/1024/1024,
					m.NumGC,
				)
			}
		}()
	}
	logMemEvery(5 * time.Second)

	scriptType := os.Getenv("SCRIPT_TYPE")
	if scriptType == "" {
		scriptType = "global_clan"
	}

	fmt.Printf("Starting tracking script: %s\n", scriptType)

	switch scriptType {
	case "global_clan":
		tracking.RunGlobalClan()
	default:
		fmt.Printf("Unknown SCRIPT_TYPE: %s\n", scriptType)
		fmt.Println("Available options: global_clan")
		os.Exit(1)
	}
}
