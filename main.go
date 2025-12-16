package main

import (
	"ClashKingTrackingGo/tracking"
	"fmt"
	"os"

	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load()

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
