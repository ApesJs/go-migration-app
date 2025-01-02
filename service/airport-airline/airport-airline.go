package airport_airline

import (
	"encoding/json"
	"fmt"
	"github.com/ApesJs/go-migration-app/database"
	"github.com/ApesJs/go-migration-app/service/airport-airline/helper"
	"github.com/schollz/progressbar/v3"
	"log"
	"os"
	"time"
)

func AirportAirlineService() {
	// Koneksi Database
	prodExistingUmrahDB := database.ConnectionProdExistingUmrahDB()
	devIdentityDB := database.ConnectionDevIdentityDB()
	devGeneralDB := database.ConnectionDevGeneralDB()
	defer devGeneralDB.Close()
	defer devIdentityDB.Close()
	defer prodExistingUmrahDB.Close()

	fmt.Printf("\nPhase 1: Migrating Airports from JSON\n")
	fmt.Printf("====================================\n")

	// Read JSON file
	jsonFile, err := os.ReadFile("service/airport-airline/seed/airport/airport-indo.json")
	if err != nil {
		log.Fatal("Error reading JSON file:", err)
	}

	// Parse JSON data
	var airports []helper.AirportJSON
	err = json.Unmarshal(jsonFile, &airports)
	if err != nil {
		log.Fatal("Error parsing JSON:", err)
	}

	totalAirports := helper.TotalAirports(airports)
	fmt.Printf("Found %d airports to transfer\n", totalAirports)

	// Prepare statements
	getCityIDStmt, err := helper.GetCityIDFromLocationStmt(devIdentityDB)
	if err != nil {
		log.Fatal("Error preparing get city ID statement:", err)
	}
	defer getCityIDStmt.Close()

	checkAirportExistStmt, err := helper.CheckAirportExistStmt(devGeneralDB)
	if err != nil {
		log.Fatal("Error preparing check airport statement:", err)
	}
	defer checkAirportExistStmt.Close()

	insertAirportStmt, err := helper.InsertAirportStmt(devGeneralDB)
	if err != nil {
		log.Fatal("Error preparing insert airport statement:", err)
	}
	defer insertAirportStmt.Close()

	// Progress bar
	bar := progressbar.NewOptions(totalAirports,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription("[cyan][1/1][reset] Transferring airports..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))

	// Statistics
	var (
		processedCount int
		insertedCount  int
		skippedCount   int
		errorCount     int
	)

	startTime := time.Now()

	// Begin transaction
	tx, err := devGeneralDB.Begin()
	if err != nil {
		log.Fatal("Error starting transaction:", err)
	}

	// Process each airport
	for _, airport := range airports {
		newID, err := helper.ProcessAirport(tx, airport, getCityIDStmt, checkAirportExistStmt, insertAirportStmt)
		if err != nil {
			log.Printf("Error processing airport %s: %v", airport.Code, err)
			errorCount++
			bar.Add(1)
			continue
		}

		if newID > 0 {
			insertedCount++
		} else {
			skippedCount++
		}
		processedCount++
		bar.Add(1)
	}

	// Commit transaction
	err = tx.Commit()
	if err != nil {
		log.Printf("Error committing transaction: %v", err)
		tx.Rollback()
		return
	}

	// Update progress bar untuk selesai
	bar.Finish()

	duration := time.Since(startTime)

	// Print summary
	fmt.Printf("\nMigration Summary:\n")
	fmt.Printf("=================\n")
	fmt.Printf("Total airports found: %d\n", totalAirports)
	fmt.Printf("Successfully processed airports: %d\n", processedCount)
	fmt.Printf("Successfully inserted airports: %d\n", insertedCount)
	fmt.Printf("Skipped (already exist): %d\n", skippedCount)
	fmt.Printf("Failed transfers: %d\n", errorCount)
	fmt.Printf("\nPerformance Metrics:\n")
	fmt.Printf("Duration: %s\n", duration.Round(time.Second))
	fmt.Printf("Average speed: %.2f airports/second\n", float64(processedCount)/duration.Seconds())
}
