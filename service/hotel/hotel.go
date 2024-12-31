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
	defer devIdentityDB.Close()
	defer devGeneralDB.Close()
	defer devUmrahDB.Close()

	// Get total number of hotels to process
	totalHotels, err := helper.TotalHotels(devUmrahDB)
	if err != nil {
		log.Fatal("Error counting hotels:", err)
	}

	fmt.Printf("Found %d total hotels to transfer\n", totalHotels)

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

	// Progress bar dengan format yang sama seperti PackageService
	bar := progressbar.NewOptions(totalHotels,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription("[cyan][1/3][reset] Transferring hotels..."),
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
			newID, err := processHotel(tx, hotel, getCityIDStmt, checkHotelExistStmt, insertHotelStmt)
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
			newID, err := processHotel(tx, hotel, getCityIDStmt, checkHotelExistStmt, insertHotelStmt)
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

	duration := time.Since(startTime)

	// Print detailed summary like PackageService
	fmt.Printf("\nHotel Migration Summary:\n")
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
	fmt.Printf("Duration: %s\n", duration.Round(time.Second))
	fmt.Printf("Average speed: %.2f hotels/second\n", float64(processedCount)/duration.Seconds())
}

func processHotel(tx *sql.Tx, hotel helper.PackageHotelJSON,
	getCityIDStmt, checkHotelExistStmt, insertHotelStmt *sql.Stmt) (int, error) {

	// Validasi data hotel
	if hotel.Name == "" {
		return 0, fmt.Errorf("hotel name is empty")
	}
	if hotel.CityName == "" {
		return 0, fmt.Errorf("city name is empty")
	}

	// Check if hotel already exists
	var existingID int
	err := checkHotelExistStmt.QueryRow(hotel.Name).Scan(&existingID)
	if err != sql.ErrNoRows {
		if err != nil {
			return 0, fmt.Errorf("error checking hotel existence: %v", err)
		}
		return 0, nil // Hotel already exists
	}

	// Get correct city_id from location_city based on cityName
	var cityID int
	err = getCityIDStmt.QueryRow(hotel.CityName).Scan(&cityID)
	if err != nil {
		return 0, fmt.Errorf("error getting city ID for city '%s': %v", hotel.CityName, err)
	}

	// Jika address kosong, gunakan default value
	address := hotel.Address
	if address == "" {
		address = "Tidak Ada"
	}

	// Insert new hotel with correct city_id
	var newID int
	err = insertHotelStmt.QueryRow(
		hotel.Name,
		address,
		hotel.CityName,
		cityID,
		hotel.Rating,
		hotel.Logo,
		hotel.CreatedAt,
		hotel.ModifiedAt,
		"migration",
		hotel.ModifiedBy,
	).Scan(&newID)

	if err != nil {
		return 0, fmt.Errorf("error inserting hotel: %v", err)
	}

	return newID, nil
}
