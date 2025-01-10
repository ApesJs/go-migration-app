package user

import (
	"database/sql"
	"fmt"
	"github.com/ApesJs/go-migration-app/database"
	"github.com/schollz/progressbar/v3"
	"log"
	"time"
)

func BdmPersonaService() {
	// Connect to databases
	prodExistingUmrahDB := database.ConnectionProdExistingUmrahDB()
	defer prodExistingUmrahDB.Close()

	//devIdentityDB := database.ConnectionDevIdentityDB()
	//defer devIdentityDB.Close()

	localIdentityDB := database.ConnectionLocalIdentityDB()
	defer localIdentityDB.Close()

	// Count total BDM users
	var totalBdmUsers int
	err := localIdentityDB.QueryRow(`SELECT COUNT(*) FROM "user" WHERE role = 'bdm'`).Scan(&totalBdmUsers)
	if err != nil {
		log.Fatal("Error counting BDM users:", err)
	}

	fmt.Printf("Found %d BDM users to process\n", totalBdmUsers)

	// Create progress bar
	bar := progressbar.NewOptions(totalBdmUsers,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription("[cyan][1/2][reset] Processing data..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
	)

	// Get BDM users from target database
	bdmRows, err := localIdentityDB.Query(`SELECT id FROM "user" WHERE role = 'bdm'`)
	if err != nil {
		log.Fatal("Error querying BDM users:", err)
	}
	defer bdmRows.Close()

	// Begin transaction
	tx, err := localIdentityDB.Begin()
	if err != nil {
		log.Fatal("Error starting transaction:", err)
	}
	defer tx.Rollback()

	// Prepare statement for checking duplicates
	checkStmt, err := tx.Prepare(`
		SELECT COUNT(*) 
		FROM "user_persona" 
		WHERE phone_number = $1 AND id != $2
	`)
	if err != nil {
		log.Fatal("Error preparing check statement:", err)
	}
	defer checkStmt.Close()

	// Prepare statement for insert/update
	insertStmt, err := tx.Prepare(`
		INSERT INTO "user_persona" (id, phone_number)
		VALUES ($1, $2)
		ON CONFLICT (id) DO UPDATE 
		SET phone_number = $2
	`)
	if err != nil {
		log.Fatal("Error preparing insert statement:", err)
	}
	defer insertStmt.Close()

	// Prepare statement for getting phone from tr_rda
	getRdaPhoneStmt, err := prodExistingUmrahDB.Prepare(`
		SELECT phone 
		FROM tr_rda 
		WHERE CAST(id AS VARCHAR(255)) = $1
	`)
	if err != nil {
		log.Fatal("Error preparing RDA phone statement:", err)
	}
	defer getRdaPhoneStmt.Close()

	// Statistics variables
	var (
		transferredCount int
		errorCount       int
		duplicateCount   int
		noPhoneCount     int
	)

	startTime := time.Now()

	// Process each BDM user
	for bdmRows.Next() {
		var userId string

		// Get BDM user ID
		err := bdmRows.Scan(&userId)
		if err != nil {
			log.Printf("Error scanning BDM user ID: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		// Get phone from tr_rda using user ID
		var phone sql.NullString
		err = getRdaPhoneStmt.QueryRow(userId).Scan(&phone)
		if err == sql.ErrNoRows {
			noPhoneCount++
			bar.Add(1)
			continue
		} else if err != nil {
			log.Printf("Error getting RDA phone: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		// Check for duplicate phone numbers if phone exists
		if phone.Valid {
			var count int
			err = checkStmt.QueryRow(phone.String, userId).Scan(&count)
			if err != nil {
				log.Printf("Error checking for duplicate phone: %v", err)
				errorCount++
				bar.Add(1)
				continue
			}

			// If duplicate found, set phone to empty string (will be NULL in database)
			if count > 0 {
				phone.String = ""
				phone.Valid = false
				duplicateCount++
			}
		}

		// Insert or update record
		_, err = insertStmt.Exec(userId, phone)
		if err != nil {
			log.Printf("Error inserting/updating row: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		transferredCount++
		bar.Add(1)
	}

	// Check for errors from bdmRows.Next()
	if err = bdmRows.Err(); err != nil {
		log.Printf("Error iterating BDM rows: %v", err)
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
	fmt.Printf("\n[2/2] Processing completed!\n")
	fmt.Printf("\nProcessing Summary:\n")
	fmt.Printf("------------------\n")
	fmt.Printf("Total BDM users: %d\n", totalBdmUsers)
	fmt.Printf("Successfully processed: %d\n", transferredCount)
	fmt.Printf("Duplicate phone numbers handled: %d\n", duplicateCount)
	fmt.Printf("No phone numbers found: %d\n", noPhoneCount)
	fmt.Printf("Failed processes: %d\n", errorCount)
	fmt.Printf("Duration: %s\n", duration.Round(time.Second))
	fmt.Printf("Average speed: %.2f records/second\n", float64(transferredCount)/duration.Seconds())
}
