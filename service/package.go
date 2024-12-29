package service

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/ApesJs/go-migration-app/database"
	"github.com/schollz/progressbar/v3"
	"log"
	"time"
)

// HotelJSON struktur untuk JSON hotel
type HotelJSON struct {
	ID         int       `json:"id"`
	Logo       string    `json:"logo"`
	Name       string    `json:"name"`
	CityID     string    `json:"cityId"`
	Rating     int       `json:"rating"`
	Address    string    `json:"address"`
	CityName   string    `json:"cityName"`
	CreatedAt  time.Time `json:"createdAt"`
	CreatedBy  string    `json:"createdBy"`
	ModifiedAt time.Time `json:"modifiedAt"`
	ModifiedBy *string   `json:"modifiedBy"`
}

func PackageService() {
	// Koneksi Database
	sourceDB, targetDB := database.ConnectionDB()
	identityDB := database.ConnectionIdentityDB()
	defer identityDB.Close()
	defer sourceDB.Close()
	defer targetDB.Close()

	// Menghitung total records yang akan ditransfer
	var totalRows int
	err := sourceDB.QueryRow(`
		SELECT COUNT(*) 
		FROM td_package 
		WHERE soft_delete = false 
		AND departure_date < CURRENT_TIMESTAMP`).Scan(&totalRows)
	if err != nil {
		log.Fatal("Error counting rows:", err)
	}

	fmt.Printf("Found %d total packages to transfer\n", totalRows)

	// Membuat progress bar
	bar := progressbar.NewOptions(totalRows,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription("[cyan][1/2][reset] Transferring packages..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
	)

	// Statement untuk mengecek organization_instance_id
	orgInstanceStmt, err := identityDB.Prepare(`
		SELECT id 
		FROM organization_instance 
		WHERE organization_id = $1
		LIMIT 1
	`)
	if err != nil {
		log.Fatal("Error preparing organization instance statement:", err)
	}
	defer orgInstanceStmt.Close()

	// Statement untuk mendapatkan nama travel
	travelStmt, err := sourceDB.Prepare(`
		SELECT name 
		FROM td_travel 
		WHERE id = $1
	`)
	if err != nil {
		log.Fatal("Error preparing organization instance statement:", err)
	}
	defer orgInstanceStmt.Close()

	// Statement untuk mengecek hotel data
	hotelStmt, err := sourceDB.Prepare(`
		SELECT h.id, h.name, h.address, h.rate, h.logo, h.created_at, h.updated_at,
			   c.id as city_id, c.name as city_name
		FROM td_package_hotel ph
		JOIN td_hotel h ON ph.hotel_id = h.id
		JOIN td_city c ON h.city_id = c.id
		WHERE ph.package_id = $1
	`)
	if err != nil {
		log.Fatal("Error preparing hotel check statement:", err)
	}
	defer hotelStmt.Close()

	// Statement untuk insert ke tabel package
	insertStmt, err := targetDB.Prepare(`
		INSERT INTO package (
			organization_id, organization_instance_id, package_type,
			thumbnail, title, description, terms_condition,
			facility, currency, medina_hotel, mecca_hotel,
			departure, arrival, dp_type, dp_amount,
			fee_type, fee_amount, deleted, created_at,
			modified_at, created_by, modified_by,
			organization_instance_name, organization_instance,
			slug
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
			$11, $12, $13, $14, $15, $16, $17, $18,
			$19, $20, $21, $22, $23, $24, $25
		)
	`)
	if err != nil {
		log.Fatal("Error preparing insert statement:", err)
	}
	defer insertStmt.Close()

	// Query untuk mengambil data package
	rows, err := sourceDB.Query(`
		SELECT id, travel_id, departure_airline_id, arrival_airline_id,
			   name, slug, image, type, share_desc, term_condition,
			   facility, currency, dp_type, dp_amount, fee_type,
			   fee_amount, soft_delete, created_at, updated_at,
			   departure_date
		FROM td_package 
		WHERE soft_delete = false 
		AND departure_date < CURRENT_TIMESTAMP
	`)
	if err != nil {
		log.Fatal("Error querying source database:", err)
	}
	defer rows.Close()

	// Struct untuk menyimpan data travel yang tidak memiliki organization_instance
	type MissingOrgInstance struct {
		TravelID   string
		TravelName string
	}

	// Variabel untuk statistik dan tracking
	var (
		transferredCount    int
		errorCount          int
		missingOrgInstances []MissingOrgInstance // Untuk menyimpan travel yang tidak memiliki organization_instance
	)

	// Begin transaction
	tx, err := targetDB.Begin()
	if err != nil {
		log.Fatal("Error starting transaction:", err)
	}

	// Prepare statements dalam transaksi
	txInsertStmt := tx.Stmt(insertStmt)

	startTime := time.Now()

	// Memproses setiap baris data
	for rows.Next() {
		var (
			id                 string
			travelID           string
			departureAirlineID string
			arrivalAirlineID   string
			name               string
			slug               sql.NullString
			image              sql.NullString
			packageType        string
			shareDesc          sql.NullString
			termCondition      sql.NullString
			facility           sql.NullString
			currency           string
			dpType             string
			dpAmount           float64
			feeType            string
			feeAmount          float64
			softDelete         bool
			createdAt          time.Time
			updatedAt          time.Time
			departureDate      time.Time
		)

		// Scan data dari source database
		err := rows.Scan(
			&id, &travelID, &departureAirlineID, &arrivalAirlineID,
			&name, &slug, &image, &packageType, &shareDesc, &termCondition,
			&facility, &currency, &dpType, &dpAmount, &feeType,
			&feeAmount, &softDelete, &createdAt, &updatedAt,
			&departureDate,
		)
		if err != nil {
			log.Printf("Error scanning row: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		// Get organization_instance_id
		var organizationInstanceID int
		err = orgInstanceStmt.QueryRow(travelID).Scan(&organizationInstanceID)
		if err != nil {
			if err == sql.ErrNoRows {
				// Jika tidak ditemukan, gunakan default value 9999
				organizationInstanceID = 9999

				// Dapatkan nama travel
				var travelName string
				err := travelStmt.QueryRow(travelID).Scan(&travelName)
				if err != nil {
					travelName = "Unknown Travel Name"
					if err != sql.ErrNoRows {
						log.Printf("Error getting travel name for ID %s: %v", travelID, err)
					}
				}

				// Tambahkan ke daftar travel yang bermasalah
				missingOrgInstances = append(missingOrgInstances, MissingOrgInstance{
					TravelID:   travelID,
					TravelName: travelName,
				})
				log.Printf("No organization_instance found for travel_id %s (%s), using default value 9999", travelID, travelName)
			} else {
				// Jika error lain selain no rows
				log.Printf("Error querying organization_instance_id for travel_id %s: %v", travelID, err)
				errorCount++
				bar.Add(1)
				continue
			}
		}

		// Get hotel data
		hotelRows, err := hotelStmt.Query(id)
		if err != nil {
			log.Printf("Error querying hotel data: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		// Collect all hotels first
		var medinaHotels, meccaHotels []*HotelJSON
		for hotelRows.Next() {
			var (
				hotelID        string
				hotelName      string
				hotelAddress   string
				hotelRate      int
				hotelLogo      string
				hotelCreatedAt time.Time
				hotelUpdatedAt time.Time
				cityID         string
				cityName       string
			)

			err := hotelRows.Scan(
				&hotelID, &hotelName, &hotelAddress, &hotelRate,
				&hotelLogo, &hotelCreatedAt, &hotelUpdatedAt,
				&cityID, &cityName,
			)
			if err != nil {
				log.Printf("Error scanning hotel row: %v", err)
				continue
			}

			hotel := &HotelJSON{
				ID:         1,
				Logo:       hotelLogo,
				Name:       hotelName,
				CityID:     cityID,
				Rating:     hotelRate,
				Address:    hotelAddress,
				CityName:   cityName,
				CreatedAt:  hotelCreatedAt,
				CreatedBy:  "migration",
				ModifiedAt: hotelUpdatedAt,
				ModifiedBy: nil,
			}

			if cityName == "Madinah" {
				medinaHotels = append(medinaHotels, hotel)
			} else if cityName == "Mekah" || cityName == "Mekkah" {
				meccaHotels = append(meccaHotels, hotel)
			}
		}
		hotelRows.Close()

		// Use the first hotel from each city if available
		var medinaHotel, meccaHotel *HotelJSON
		if len(medinaHotels) > 0 {
			medinaHotel = medinaHotels[0]
		}
		if len(meccaHotels) > 0 {
			meccaHotel = meccaHotels[0]
		}

		// Convert hotels to JSON
		medinaHotelJSON, err := json.Marshal(medinaHotel)
		if err != nil {
			log.Printf("Error marshaling medina hotel: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		meccaHotelJSON, err := json.Marshal(meccaHotel)
		if err != nil {
			log.Printf("Error marshaling mecca hotel: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		// Convert package type
		finalPackageType := "umrah"
		if packageType == "2" {
			finalPackageType = "hajj"
		}

		// Default organization instance JSON
		orgInstanceJSON := []byte(`{"status": "belum ada"}`)

		// Convert departure and arrival to default JSON for now
		defaultAirlineJSON := []byte("{}")

		_, err = txInsertStmt.Exec(
			travelID,               // organization_id
			organizationInstanceID, // organization_instance_id
			finalPackageType,       // package_type
			image.String,           // thumbnail
			name,                   // title
			shareDesc.String,       // description
			termCondition.String,   // terms_condition
			facility.String,        // facility
			currency,               // currency
			medinaHotelJSON,        // medina_hotel
			meccaHotelJSON,         // mecca_hotel
			defaultAirlineJSON,     // departure
			defaultAirlineJSON,     // arrival
			dpType,                 // dp_type
			int(dpAmount),          // dp_amount
			feeType,                // fee_type
			int(feeAmount),         // fee_amount
			softDelete,             // deleted
			createdAt,              // created_at
			updatedAt,              // modified_at
			"migration",            // created_by
			nil,                    // modified_by
			"test",                 // organization_instance_name
			orgInstanceJSON,        // organization_instance
			slug.String,            // slug
		)
		if err != nil {
			log.Printf("Error inserting row: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		transferredCount++
		bar.Add(1)
	}

	// Commit transaction
	err = tx.Commit()
	if err != nil {
		log.Printf("Error committing transaction: %v", err)
		tx.Rollback()
		return
	}

	duration := time.Since(startTime)

	// Update progress bar description for completion
	bar.Finish()
	fmt.Printf("\n[2/2] Transfer completed!\n")
	fmt.Printf("\nTransfer Summary:\n")
	fmt.Printf("----------------\n")
	fmt.Printf("Total records: %d\n", totalRows)
	fmt.Printf("Successfully transferred: %d\n", transferredCount)
	fmt.Printf("Failed transfers: %d\n", errorCount)
	fmt.Printf("Duration: %s\n", duration.Round(time.Second))
	fmt.Printf("Average speed: %.2f records/second\n", float64(transferredCount)/duration.Seconds())

	// Tampilkan daftar travel_id yang tidak memiliki organization_instance
	if len(missingOrgInstances) > 0 {
		fmt.Printf("\nTravels without organization instance (using default value 9999):\n")
		fmt.Printf("-------------------------------------------------------\n")
		for i, travel := range missingOrgInstances {
			fmt.Printf("%d. ID: %s\n   Name: %s\n", i+1, travel.TravelID, travel.TravelName)
		}
		fmt.Printf("\nTotal missing organization instances: %d\n", len(missingOrgInstances))
	}
}
