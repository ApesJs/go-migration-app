package airline

import (
	"fmt"
	"github.com/ApesJs/go-migration-app/database"
	"github.com/ApesJs/go-migration-app/service/airline/helper"
	"github.com/schollz/progressbar/v3"
	"log"
	"strconv"
)

func AirlineService() {
	// Koneksi Database
	devGeneralDB := database.ConnectionDevGeneralDB()
	devUmrahDB := database.ConnectionDevUmrahDB()
	defer devGeneralDB.Close()
	defer devUmrahDB.Close()

	// Prepare statements untuk airline
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

	// Prepare statements untuk update package
	getNewIDStmt, err := helper.GetNewAirlineIDStmt(devGeneralDB)
	if err != nil {
		log.Fatal("Error preparing get new ID statement:", err)
	}
	defer getNewIDStmt.Close()

	updateDepartureStmt, err := helper.UpdatePackageDepartureAirlineStmt(devUmrahDB)
	if err != nil {
		log.Fatal("Error preparing update departure statement:", err)
	}
	defer updateDepartureStmt.Close()

	updateArrivalStmt, err := helper.UpdatePackageArrivalAirlineStmt(devUmrahDB)
	if err != nil {
		log.Fatal("Error preparing update arrival statement:", err)
	}
	defer updateArrivalStmt.Close()

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

	// Create progress bar untuk insert airline
	bar := progressbar.NewOptions(len(allAirlines),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription("[cyan][1/2][reset] Inserting airlines..."),
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
		updateCount  int
		updateErrors int
	)

	// Begin transaction untuk airline
	tx, err := devGeneralDB.Begin()
	if err != nil {
		log.Fatal("Error starting transaction:", err)
	}

	// Process each airline
	for _, airline := range allAirlines {
		// Check if airline already exists
		var count int
		err := checkStmt.QueryRow(airline.Code).Scan(&count)
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
		_, err = insertStmt.Exec(
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

	// Commit transaction airline
	err = tx.Commit()
	if err != nil {
		log.Printf("Error committing transaction: %v", err)
		tx.Rollback()
		return
	}

	// Create progress bar untuk update package
	updateBar := progressbar.NewOptions(len(allAirlines),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription("[cyan][2/2][reset] Updating package references..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))

	// Begin transaction untuk update package
	txUmrah, err := devUmrahDB.Begin()
	if err != nil {
		log.Fatal("Error starting package update transaction:", err)
	}

	// Prepare statements dalam transaction
	txUpdateDepartureStmt := txUmrah.Stmt(updateDepartureStmt)
	txUpdateArrivalStmt := txUmrah.Stmt(updateArrivalStmt)

	// Update references di tabel package
	for _, airline := range allAirlines {
		// Get new airline ID
		var newID int
		err := getNewIDStmt.QueryRow(airline.Code).Scan(&newID)
		if err != nil {
			log.Printf("Error getting new ID for airline %s: %v", airline.Code, err)
			updateErrors++
			updateBar.Add(1)
			continue
		}

		// Convert ID to JSON string
		idJSON := strconv.Itoa(newID)

		// Update departure references berdasarkan nama airline
		result, err := txUpdateDepartureStmt.Exec(idJSON, airline.Name)
		if err != nil {
			log.Printf("Error updating departure references for airline %s: %v", airline.Name, err)
			updateErrors++
		} else {
			rows, _ := result.RowsAffected()
			updateCount += int(rows)
		}

		// Update arrival references berdasarkan nama airline
		result, err = txUpdateArrivalStmt.Exec(idJSON, airline.Name)
		if err != nil {
			log.Printf("Error updating arrival references for airline %s: %v", airline.Name, err)
			updateErrors++
		} else {
			rows, _ := result.RowsAffected()
			updateCount += int(rows)
		}

		updateBar.Add(1)
	}

	// Commit transaction update package
	err = txUmrah.Commit()
	if err != nil {
		log.Printf("Error committing package update transaction: %v", err)
		txUmrah.Rollback()
		return
	}

	// Print summary
	fmt.Printf("\nMigration Summary:\n")
	fmt.Printf("----------------\n")
	fmt.Printf("Airlines:\n")
	fmt.Printf("  Total processed: %d\n", len(allAirlines))
	fmt.Printf("  Successfully inserted: %d\n", successCount)
	fmt.Printf("  Skipped (already exists): %d\n", skipCount)
	fmt.Printf("  Failed: %d\n", errorCount)
	fmt.Printf("\nPackage Updates:\n")
	fmt.Printf("  References updated: %d\n", updateCount)
	fmt.Printf("  Update errors: %d\n", updateErrors)
}
