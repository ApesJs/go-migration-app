package helper

import (
	"fmt"
	"time"
)

func PrintSummary(stats *TransferStats, totalRows, totalTravelAgents int, duration time.Duration) {
	fmt.Printf("\n[2/2] Transfer completed!\n")
	fmt.Printf("\nTransfer Summary:\n")
	fmt.Printf("----------------\n")
	fmt.Printf("Total records: %d\n", totalRows)
	fmt.Printf("Total travel agents in source: %d\n", totalTravelAgents)
	fmt.Printf("Successfully transferred: %d\n", stats.TransferredCount)
	fmt.Printf("Converted to wukala: %d\n", stats.WukalaCount)
	fmt.Printf("Skipped (duplicates): %d\n", stats.SkipCount)
	fmt.Printf("Failed transfers: %d\n", stats.ErrorCount)
	fmt.Printf("Duration: %s\n", duration.Round(time.Second))
	fmt.Printf("Average speed: %.2f records/second\n", float64(stats.TransferredCount)/duration.Seconds())

	printDuplicateEmails(stats.DuplicateEmails)
	printSkippedTravelAgents(stats)
}

func printDuplicateEmails(duplicateEmails []string) {
	if len(duplicateEmails) > 0 {
		fmt.Printf("\nDuplicate Emails:\n")
		fmt.Printf("----------------\n")
		for i, email := range duplicateEmails {
			fmt.Printf("%d. %s\n", i+1, email)
		}
	}
}

func printSkippedTravelAgents(stats *TransferStats) {
	if len(stats.SkippedTravelAgent) > 0 {
		fmt.Printf("\nSkipped Travel Agents (already exists):\n")
		fmt.Printf("------------------------------------\n")
		i := 1
		for email, name := range stats.SkippedTravelAgent {
			fmt.Printf("%d. %s (%s)\n", i, email, name)
			i++
		}
		fmt.Printf("\nTotal travel agents skipped: %d\n", len(stats.SkippedTravelAgent))
	}
}
