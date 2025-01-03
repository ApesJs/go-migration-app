package airline

import (
	"fmt"
	"github.com/ApesJs/go-migration-app/database"
	"github.com/ApesJs/go-migration-app/service/airline/helper"
	"github.com/schollz/progressbar/v3"
	"log"
)

func AirlineService() {
	// Koneksi Database
	devGeneralDB := database.ConnectionDevGeneralDB()
	devUmrahDB := database.ConnectionDevUmrahDB()
	defer devGeneralDB.Close()
	defer devUmrahDB.Close()

	// Prepare statements
	insertStmt, err := helper.InsertAirlineStmt(devGeneralDB)
	if err != nil {
		log.Fatal("Error preparing insert statement:", err)
	}
	defer insertStmt.Close()

	checkStmt, err := helper.CheckExistingAirlineStmt(devGeneralDB)
	if err != nil {
		log.Fatal("Error preparing check statement:", err)
	}
	defer checkStmt.Close()

	// Read Indonesian airlines
	indoAirlines, err := helper.ReadAirlineJSON("service/airline/seed/airline/airline-indo.json")
	if err != nil {
		log.Fatal("Error reading Indonesian airlines:", err)
	}

	// Read Arab airlines
	arabAirlines, err := helper.ReadAirlineJSON("service/airline/seed/airline/airline-arab.json")
	if err != nil {
		log.Fatal("Error reading Arab airlines:", err)
	}

	// Process data with country information
	allAirlines := []helper.AirlineWithCountry{}
	allAirlines = append(allAirlines,
		helper.ProcessAirlineData(indoAirlines, "INDONESIA", "360")...)
	allAirlines = append(allAirlines,
		helper.ProcessAirlineData(arabAirlines, "ARAB SAUDI", "682")...)

	// Create progress bar
	bar := progressbar.NewOptions(len(allAirlines),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription("[cyan][1/1][reset] Inserting airlines..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))

	// Statistics
	var (
		successCount int
		errorCount   int
		skipCount    int
	)

	// Begin transaction
	tx, err := devGeneralDB.Begin()
	if err != nil {
		log.Fatal("Error starting transaction:", err)
	}

	// Prepare statement within transaction
	txInsertStmt := tx.Stmt(insertStmt)
	txCheckStmt := tx.Stmt(checkStmt)

	// Process each airline
	for _, airline := range allAirlines {
		// Check if airline already exists
		var count int
		err := txCheckStmt.QueryRow(airline.Code).Scan(&count)
		if err != nil {
			log.Printf("Error checking existing airline %s: %v", airline.Code, err)
			errorCount++
			bar.Add(1)
			continue
		}

		if count > 0 {
			skipCount++
			bar.Add(1)
			continue
		}

		// Insert new airline
		_, err = txInsertStmt.Exec(
			airline.Name,        // name
			airline.Code,        // code
			airline.CountryName, // country_name
			airline.CountryID,   // country_id
			nil,                 // logo
			"migration",         // created_by
			nil,                 // modified_by
		)

		if err != nil {
			log.Printf("Error inserting airline %s: %v", airline.Code, err)
			errorCount++
		} else {
			successCount++
		}
		bar.Add(1)
	}

	// Commit transaction
	err = tx.Commit()
	if err != nil {
		log.Printf("Error committing transaction: %v", err)
		tx.Rollback()
		return
	}

	// Print summary
	fmt.Printf("\nMigration Summary:\n")
	fmt.Printf("----------------\n")
	fmt.Printf("Total airlines processed: %d\n", len(allAirlines))
	fmt.Printf("Successfully inserted: %d\n", successCount)
	fmt.Printf("Skipped (already exists): %d\n", skipCount)
	fmt.Printf("Failed: %d\n", errorCount)
}
