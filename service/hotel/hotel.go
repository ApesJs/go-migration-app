// hotel.go
package hotel

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/ApesJs/go-migration-app/database"
	"github.com/ApesJs/go-migration-app/service/hotel/helper"
	"github.com/schollz/progressbar/v3"
	"log"
	"time"
)

func HotelService() {
	// Koneksi Database
	devUmrahDB := database.ConnectionDevUmrahDB()
	devGeneralDB := database.ConnectionDevGeneralDB()
	devIdentityDB := database.ConnectionDevIdentityDB()
	prodExistingUmrahDB := database.ConnectionProdExistingUmrahDB()
	defer prodExistingUmrahDB.Close()
	defer devIdentityDB.Close()
	defer devGeneralDB.Close()
	defer devUmrahDB.Close()

	fmt.Printf("\nPhase 1: Migrating Hotels from Package Table\n")
	fmt.Printf("==========================================\n")

	// Get total number of hotels to process
	totalHotels, err := helper.TotalHotels(devUmrahDB)
	if err != nil {
		log.Fatal("Error counting hotels:", err)
	}

	fmt.Printf("Found %d total hotels to transfer from package\n", totalHotels)

	// Prepare statements
	getAllPackageHotelsStmt, err := helper.GetAllPackageHotelsStmt(devUmrahDB)
	if err != nil {
		log.Fatal("Error preparing get all package hotels statement:", err)
	}
	defer getAllPackageHotelsStmt.Close()

	getCityIDStmt, err := helper.GetCityIDStmt(devIdentityDB)
	if err != nil {
		log.Fatal("Error preparing get city ID statement:", err)
	}
	defer getCityIDStmt.Close()

	checkHotelExistStmt, err := helper.CheckHotelExistStmt(devGeneralDB)
	if err != nil {
		log.Fatal("Error preparing check hotel statement:", err)
	}
	defer checkHotelExistStmt.Close()

	insertHotelStmt, err := helper.InsertHotelStmt(devGeneralDB)
	if err != nil {
		log.Fatal("Error preparing insert hotel statement:", err)
	}
	defer insertHotelStmt.Close()

	updateMedinaHotelStmt, err := helper.UpdatePackageHotelIDStmt(devUmrahDB, "medina_hotel")
	if err != nil {
		log.Fatal("Error preparing update medina hotel statement:", err)
	}
	defer updateMedinaHotelStmt.Close()

	updateMeccaHotelStmt, err := helper.UpdatePackageHotelIDStmt(devUmrahDB, "mecca_hotel")
	if err != nil {
		log.Fatal("Error preparing update mecca hotel statement:", err)
	}
	defer updateMeccaHotelStmt.Close()

	// Progress bar untuk Phase 1
	bar := progressbar.NewOptions(totalHotels,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription("[cyan][1/3][reset] Transferring package hotels..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))

	// Statistics untuk Phase 1
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

	// Get all package hotels
	rows, err := getAllPackageHotelsStmt.Query()
	if err != nil {
		log.Fatal("Error querying package hotels:", err)
	}
	defer rows.Close()

	// Process each row
	for rows.Next() {
		var medinaHotelJSON, meccaHotelJSON sql.NullString
		err := rows.Scan(&medinaHotelJSON, &meccaHotelJSON)
		if err != nil {
			log.Printf("Error scanning row: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		// Process Medina Hotel
		if medinaHotelJSON.Valid && medinaHotelJSON.String != "" && medinaHotelJSON.String != "null" {
			var hotel helper.PackageHotelJSON
			err = json.Unmarshal([]byte(medinaHotelJSON.String), &hotel)
			if err != nil {
				log.Printf("Error unmarshaling medina hotel: %v, JSON: %s", err, medinaHotelJSON.String)
				errorCount++
				bar.Add(1)
				continue
			}

			// Log data hotel yang bermasalah
			if hotel.CityName == "" {
				log.Printf("Found medina hotel with empty city name: %+v", hotel)
				errorCount++
				bar.Add(1)
				continue
			}

			// Process hotel
			newID, err := helper.ProcessHotel(tx, hotel, getCityIDStmt, checkHotelExistStmt, insertHotelStmt)
			if err != nil {
				log.Printf("Error processing medina hotel: %v, Hotel Data: %+v", err, hotel)
				errorCount++
				bar.Add(1)
				continue
			}

			if newID > 0 {
				// Update package with new hotel ID
				_, err = updateMedinaHotelStmt.Exec(newID, hotel.Name)
				if err != nil {
					log.Printf("Error updating medina hotel ID: %v", err)
					errorCount++
				}
				insertedCount++
			} else {
				skippedCount++
			}
			processedCount++
		}

		// Process Mecca Hotel
		if meccaHotelJSON.Valid && meccaHotelJSON.String != "" && meccaHotelJSON.String != "null" {
			var hotel helper.PackageHotelJSON
			err = json.Unmarshal([]byte(meccaHotelJSON.String), &hotel)
			if err != nil {
				log.Printf("Error unmarshaling mecca hotel: %v", err)
				errorCount++
				bar.Add(1)
				continue
			}

			// Process hotel
			newID, err := helper.ProcessHotel(tx, hotel, getCityIDStmt, checkHotelExistStmt, insertHotelStmt)
			if err != nil {
				log.Printf("Error processing mecca hotel: %v", err)
				errorCount++
				bar.Add(1)
				continue
			}

			if newID > 0 {
				// Update package with new hotel ID
				_, err = updateMeccaHotelStmt.Exec(newID, hotel.Name)
				if err != nil {
					log.Printf("Error updating mecca hotel ID: %v", err)
					errorCount++
				}
				insertedCount++
			} else {
				skippedCount++
			}
			processedCount++
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

	// Update progress bar untuk standardisasi nama kota
	bar.Finish()
	fmt.Printf("\n[2/3] Standardizing hotel names...\n")

	// Update hotel images to include full URL
	fmt.Printf("\n[3/3] Updating hotel image URLs...\n")

	updateMedinaImageResult, err := devUmrahDB.Exec(`
        UPDATE package
        SET medina_hotel = jsonb_set(
            medina_hotel,
            '{logo}',
            CASE 
                WHEN medina_hotel->>'logo' NOT LIKE 'https://%'
                THEN concat('"https://sin1.contabostorage.com/8e42befee02d4023b5b53cb887ef1d70:umroh-prod/', medina_hotel->>'logo', '"')::jsonb
                ELSE to_jsonb(medina_hotel->>'logo')
            END
        )
        WHERE medina_hotel->>'logo' IS NOT NULL AND medina_hotel->>'logo' != ''
    `)
	if err != nil {
		log.Printf("Error updating Madinah hotel images: %v", err)
	}

	updateMeccaImageResult, err := devUmrahDB.Exec(`
        UPDATE package
        SET mecca_hotel = jsonb_set(
            mecca_hotel,
            '{logo}',
            CASE 
                WHEN mecca_hotel->>'logo' NOT LIKE 'https://%'
                THEN concat('"https://sin1.contabostorage.com/8e42befee02d4023b5b53cb887ef1d70:umroh-prod/', mecca_hotel->>'logo', '"')::jsonb
                ELSE to_jsonb(mecca_hotel->>'logo')
            END
        )
        WHERE mecca_hotel->>'logo' IS NOT NULL AND mecca_hotel->>'logo' != ''
    `)
	if err != nil {
		log.Printf("Error updating Mecca hotel images: %v", err)
	}

	// Get affected rows
	medinaImageRowsAffected, _ := updateMedinaImageResult.RowsAffected()
	meccaImageRowsAffected, _ := updateMeccaImageResult.RowsAffected()

	durationPhase1 := time.Since(startTime)

	// Print summary untuk Phase 1
	fmt.Printf("\nPhase 1 Migration Summary:\n")
	fmt.Printf("------------------------\n")
	fmt.Printf("Total hotels found: %d\n", totalHotels)
	fmt.Printf("Successfully processed hotels: %d\n", processedCount)
	fmt.Printf("Successfully inserted hotels: %d\n", insertedCount)
	fmt.Printf("Skipped (already exist): %d\n", skippedCount)
	fmt.Printf("Failed transfers: %d\n", errorCount)
	fmt.Printf("\nStandardization Results:\n")
	fmt.Printf("Updated Madinah hotel images: %d\n", medinaImageRowsAffected)
	fmt.Printf("Updated Makkah hotel images: %d\n", meccaImageRowsAffected)
	fmt.Printf("\nPerformance Metrics:\n")
	fmt.Printf("Duration: %s\n", durationPhase1.Round(time.Second))
	fmt.Printf("Average speed: %.2f hotels/second\n", float64(processedCount)/durationPhase1.Seconds())

	// Phase 2: Migrasi dari td_hotel
	fmt.Printf("\nPhase 2: Migrating Hotels from td_hotel Table\n")
	fmt.Printf("==========================================\n")

	// Get total hotels from td_hotel
	var totalTdHotels int
	err = prodExistingUmrahDB.QueryRow(`
        SELECT COUNT(*) 
        FROM td_hotel
        WHERE soft_delete = false
    `).Scan(&totalTdHotels)
	if err != nil {
		log.Fatal("Error counting td_hotels:", err)
	}

	fmt.Printf("Found %d hotels to transfer from td_hotel\n", totalTdHotels)

	// Progress bar untuk Phase 2
	barPhase2 := progressbar.NewOptions(totalTdHotels,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription("[cyan][1/2][reset] Transferring td_hotels..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))

	// Statistics untuk Phase 2
	var (
		processedTdCount int
		insertedTdCount  int
		skippedTdCount   int
		errorTdCount     int
	)

	startTimeTd := time.Now()

	// Begin transaction untuk Phase 2
	txTd, err := devGeneralDB.Begin()
	if err != nil {
		log.Fatal("Error starting transaction for td_hotel:", err)
	}

	// Query untuk mengambil data dari td_hotel dengan city name
	tdHotelRows, err := prodExistingUmrahDB.Query(`
        SELECT 
            h.name,
            h.address,
            h.rate,
            h.logo,
            h.created_at,
            h.updated_at,
            c.name as city_name
        FROM td_hotel h
        JOIN td_city c ON h.city_id = c.id
        WHERE h.soft_delete = false
    `)
	if err != nil {
		log.Fatal("Error querying td_hotel:", err)
	}
	defer tdHotelRows.Close()

	// Process setiap hotel dari td_hotel
	for tdHotelRows.Next() {
		var (
			name      string
			address   string
			rate      int
			logo      string
			createdAt time.Time
			updatedAt time.Time
			cityName  string
		)

		err := tdHotelRows.Scan(
			&name,
			&address,
			&rate,
			&logo,
			&createdAt,
			&updatedAt,
			&cityName,
		)
		if err != nil {
			log.Printf("Error scanning td_hotel row: %v", err)
			errorTdCount++
			barPhase2.Add(1)
			continue
		}

		// Check if hotel already exists
		var existingID int
		err = checkHotelExistStmt.QueryRow(name).Scan(&existingID)
		if err != sql.ErrNoRows {
			if err != nil {
				log.Printf("Error checking hotel existence: %v", err)
				errorTdCount++
			} else {
				skippedTdCount++
			}
			barPhase2.Add(1)
			continue
		}

		// Insert new hotel
		var newID int
		err = insertHotelStmt.QueryRow(
			name,        // name
			address,     // address
			cityName,    // city_name
			1,           // city_id (default to 1)
			rate,        // rating
			logo,        // logo
			createdAt,   // created_at
			updatedAt,   // modified_at
			"migration", // created_by
			nil,         // modified_by
		).Scan(&newID)

		if err != nil {
			log.Printf("Error inserting hotel: %v", err)
			errorTdCount++
			barPhase2.Add(1)
			continue
		}

		insertedTdCount++
		processedTdCount++
		barPhase2.Add(1)
	}

	// Commit transaction Phase 2
	err = txTd.Commit()
	if err != nil {
		log.Printf("Error committing td_hotel transaction: %v", err)
		txTd.Rollback()
		return
	}

	// Update progress bar untuk selesai
	barPhase2.Finish()

	// Standardisasi nama kota Mekah/Mekkah
	updateMeccaResult, err := devGeneralDB.Exec(`
		UPDATE hotel
		SET city_name = "MAKKAH"
		WHERE city_name = 'Mekkah'
		OR city_name = 'Mekah'
	`)
	if err != nil {
		log.Printf("Error updating Mecca city names: %v", err)
	}

	// Standardisasi nama kota Madinah
	updateMadinahResult, err := devGeneralDB.Exec(`
		UPDATE hotel
		SET city_name = "MADINAH"
		WHERE city_name = 'Madinah'
	`)
	if err != nil {
		log.Printf("Error updating Madinah city names: %v", err)
	}

	meccaRowsAffected, _ := updateMeccaResult.RowsAffected()
	madinahRowsAffected, _ := updateMadinahResult.RowsAffected()

	fmt.Printf("\n[3/3] City name standardization completed!\n")
	fmt.Printf("Standardized %d Mecca hotel records\n", meccaRowsAffected)
	fmt.Printf("Standardized %d Madinah hotel records\n", madinahRowsAffected)

	durationTd := time.Since(startTimeTd)

	// Print summary untuk Phase 2
	fmt.Printf("\nPhase 2 Migration Summary:\n")
	fmt.Printf("------------------------\n")
	fmt.Printf("Total td_hotels found: %d\n", totalTdHotels)
	fmt.Printf("Successfully processed hotels: %d\n", processedTdCount)
	fmt.Printf("Successfully inserted hotels: %d\n", insertedTdCount)
	fmt.Printf("Skipped (already exist): %d\n", skippedTdCount)
	fmt.Printf("Failed transfers: %d\n", errorTdCount)
	fmt.Printf("\nPerformance Metrics:\n")
	fmt.Printf("Duration: %s\n", durationTd.Round(time.Second))
	fmt.Printf("Average speed: %.2f hotels/second\n", float64(processedTdCount)/durationTd.Seconds())

	// Print overall summary
	fmt.Printf("\nOverall Migration Summary:\n")
	fmt.Printf("======================\n")
	fmt.Printf("Total Phase 1 - Package Hotels: %d\n", processedCount)
	fmt.Printf("Total Phase 2 - TD Hotels: %d\n", processedTdCount)
	fmt.Printf("Total Hotels Processed: %d\n", processedCount+processedTdCount)
	fmt.Printf("Total Hotels Inserted: %d\n", insertedCount+insertedTdCount)
	fmt.Printf("Total Hotels Skipped: %d\n", skippedCount+skippedTdCount)
	fmt.Printf("Total Errors: %d\n", errorCount+errorTdCount)
}
