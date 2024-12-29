package service

import (
	"database/sql"
	"fmt"
	"github.com/ApesJs/go-migration-app/database"
	"github.com/schollz/progressbar/v3"
	"log"
	"time"
)

func ChangeCityIDPersonaService() {
	// Connect to databases
	prodExistingUmrahDB := database.ConnectionProdExistingUmrahDB()
	devIdentityDB := database.ConnectionDevIdentityDB()
	defer prodExistingUmrahDB.Close()
	defer devIdentityDB.Close()

	fmt.Println("Starting City ID conversion process...")

	// Step 1: Alter table to change city_id type to varchar
	_, err := devIdentityDB.Exec(`
		ALTER TABLE "user-persona" 
		ALTER COLUMN city_id TYPE varchar
		USING city_id::varchar
	`)
	if err != nil {
		log.Fatal("Error altering city_id column type:", err)
	}
	fmt.Println("Successfully altered city_id column type to varchar")

	// Count total records to be processed
	var totalRows int
	err = devIdentityDB.QueryRow(`SELECT COUNT(*) FROM "user-persona" WHERE city_id IS NOT NULL`).Scan(&totalRows)
	if err != nil {
		log.Fatal("Error counting rows:", err)
	}

	fmt.Printf("Found %d records with city_id to process\n", totalRows)

	if totalRows == 0 {
		fmt.Println("No records to process")
		return
	}

	// Create progress bar
	bar := progressbar.NewOptions(totalRows,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription("[cyan][1/2][reset] Converting city IDs..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
	)

	// Begin transaction
	tx, err := devIdentityDB.Begin()
	if err != nil {
		log.Fatal("Error starting transaction:", err)
	}
	defer tx.Rollback()

	// Prepare statements
	getCityNameStmt, err := prodExistingUmrahDB.Prepare(`
		SELECT name 
		FROM td_city 
		WHERE id = $1
	`)
	if err != nil {
		log.Fatal("Error preparing get city name statement:", err)
	}
	defer getCityNameStmt.Close()

	updateCityStmt, err := tx.Prepare(`
		UPDATE "user-persona"
		SET city_id = $1
		WHERE id = $2
	`)
	if err != nil {
		log.Fatal("Error preparing update statement:", err)
	}
	defer updateCityStmt.Close()

	// Get all user-persona records with city_id
	rows, err := devIdentityDB.Query(`
		SELECT id, city_id 
		FROM "user-persona" 
		WHERE city_id IS NOT NULL
	`)
	if err != nil {
		log.Fatal("Error querying user-persona records:", err)
	}
	defer rows.Close()

	// Statistics variables
	var (
		updatedCount  int
		errorCount    int
		notFoundCount int
	)

	startTime := time.Now()

	// Process each record
	for rows.Next() {
		var (
			id     string
			cityID string
		)

		// Scan user-persona record
		err := rows.Scan(&id, &cityID)
		if err != nil {
			log.Printf("Error scanning row: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		// Get city name from source database
		var cityName string
		err = getCityNameStmt.QueryRow(cityID).Scan(&cityName)
		if err == sql.ErrNoRows {
			log.Printf("City not found for ID %s", cityID)
			notFoundCount++
			bar.Add(1)
			continue
		} else if err != nil {
			log.Printf("Error getting city name: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		// Update user-persona with city name
		_, err = updateCityStmt.Exec(cityName, id)
		if err != nil {
			log.Printf("Error updating city: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		updatedCount++
		bar.Add(1)
	}

	// Check for errors from rows.Next()
	if err = rows.Err(); err != nil {
		log.Printf("Error iterating rows: %v", err)
		return
	}

	// Commit transaction
	err = tx.Commit()
	if err != nil {
		log.Printf("Error committing transaction: %v", err)
		return
	}

	duration := time.Since(startTime)

	// Update progress bar description for completion
	bar.Finish()
	fmt.Printf("\n[2/2] Conversion completed!\n")
	fmt.Printf("\nConversion Summary:\n")
	fmt.Printf("------------------\n")
	fmt.Printf("Total records processed: %d\n", totalRows)
	fmt.Printf("Successfully updated: %d\n", updatedCount)
	fmt.Printf("Cities not found: %d\n", notFoundCount)
	fmt.Printf("Failed updates: %d\n", errorCount)
	fmt.Printf("Duration: %s\n", duration.Round(time.Second))
	fmt.Printf("Average speed: %.2f records/second\n", float64(updatedCount)/duration.Seconds())
}
