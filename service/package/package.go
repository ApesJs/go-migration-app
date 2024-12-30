package _package

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/ApesJs/go-migration-app/database"
	"github.com/ApesJs/go-migration-app/service/package/helper"
	"github.com/schollz/progressbar/v3"
	"log"
	"time"
)

func PackageService() {
	// Koneksi Database
	prodExistingUmrahDB := database.ConnectionProdExistingUmrahDB()
	localUmrahDB := database.ConnectionLocalUmrahDB()
	devIdentityDB := database.ConnectionDevIdentityDB()
	defer devIdentityDB.Close()
	defer prodExistingUmrahDB.Close()
	defer localUmrahDB.Close()

	// Menghitung total records yang akan ditransfer
	totalRows := helper.TotalRows(prodExistingUmrahDB)

	fmt.Printf("Found %d total packages to transfer\n", totalRows)

	// Membuat progress bar
	bar := progressbar.NewOptions(totalRows,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription("[cyan][1/3][reset] Transferring packages..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
	)

	// Statement untuk mengecek organization_instance_id
	orgInstanceStmt, err := devIdentityDB.Prepare(`
		SELECT id, name 
		FROM organization_instance 
		WHERE organization_id = $1
		LIMIT 1
	`)
	if err != nil {
		log.Fatal("Error preparing organization instance statement:", err)
	}
	defer orgInstanceStmt.Close()

	// Statement untuk mendapatkan nama travel
	travelStmt, err := prodExistingUmrahDB.Prepare(`
		SELECT name 
		FROM td_travel 
		WHERE id = $1
	`)
	if err != nil {
		log.Fatal("Error preparing organization instance statement:", err)
	}
	defer orgInstanceStmt.Close()

	// Statement untuk mengecek hotel data
	hotelStmt, err := prodExistingUmrahDB.Prepare(`
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

	// Statement untuk mengambil data airline
	airlineStmt, err := prodExistingUmrahDB.Prepare(`
        SELECT code, logo, name, created_at, updated_at
        FROM td_airline 
        WHERE id = $1
    `)
	if err != nil {
		log.Fatal("Error preparing airline statement:", err)
	}
	defer airlineStmt.Close()

	// Statement untuk insert ke tabel package
	insertStmt, err := localUmrahDB.Prepare(`
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
	rows, err := prodExistingUmrahDB.Query(`
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

	// Variabel untuk statistik dan tracking
	var (
		transferredCount    int
		errorCount          int
		missingOrgInstances []helper.MissingOrgInstance // Untuk menyimpan travel yang tidak memiliki organization_instance
	)

	// Begin transaction
	tx, err := localUmrahDB.Begin()
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
		var (
			organizationInstanceID   int
			organizationInstanceName string = "Nama Travel Tidak di Temukan"
		)
		err = orgInstanceStmt.QueryRow(travelID).Scan(&organizationInstanceID, &organizationInstanceName)
		if err != nil {
			if err == sql.ErrNoRows {
				// Jika tidak ditemukan, gunakan default value 9999
				organizationInstanceID = 9999
				organizationInstanceName = "Nama Travel Tidak di Temukan"

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
				missingOrgInstances = append(missingOrgInstances, helper.MissingOrgInstance{
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

		var orgInstanceJSON []byte
		if organizationInstanceID != 9999 {
			apiResponse, err := helper.GetOrganizationInstance(travelID, organizationInstanceID)
			if err != nil {
				log.Printf("Error getting organization instance data: %v", err)
				orgInstanceJSON = []byte(`{"status": "error fetching data"}`)
			} else {
				orgInstanceJSON = apiResponse
			}
		} else {
			// Gunakan default JSON jika tidak ada organization instance
			orgInstanceJSON = []byte(`{"status": "belum ada"}`)
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
		var medinaHotels, meccaHotels []*helper.HotelJSON
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

			hotel := &helper.HotelJSON{
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
		var medinaHotel, meccaHotel *helper.HotelJSON
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

		// Create departure JSON
		departureCreatedBy := "643aaa6d-7caa-4c3c-99b5-d062447c3d3a"
		//var departureModifiedBy *string = nil

		// Get airline data for departure
		var (
			airlineCode      string
			airlineLogo      sql.NullString
			airlineName      string
			airlineCreatedAt time.Time
			airlineUpdatedAt time.Time
		)

		err = airlineStmt.QueryRow(departureAirlineID).Scan(
			&airlineCode,
			&airlineLogo,
			&airlineName,
			&airlineCreatedAt,
			&airlineUpdatedAt,
		)
		if err != nil && err != sql.ErrNoRows {
			log.Printf("Error getting airline data: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		// Create departure flight JSON
		departureFlight := helper.FlightJSON{
			To: helper.AirportWrapperJSON{
				Airport: helper.CreateAirportJSON(
					6,                              // id
					"JED",                          // code
					"Internasional King Abdulaziz", // name
					"0213",                         // cityId
					"JEDDAH",                       // cityName
					"682",                          // countryId
					time.Date(2024, 12, 28, 16, 35, 56, 423000000, time.UTC), // createdAt
					time.Date(2024, 12, 28, 16, 35, 56, 475415000, time.UTC), // modifiedAt
					departureCreatedBy, // createdBy
					nil,                // modifiedBy
					"JEDDAH",           // countryName
				),
				AirportID: 6,
			},
			From: helper.AirportWrapperJSON{
				Airport: helper.CreateAirportJSON(
					3,                        // id
					"SOE",                    // code
					"Soekarno Hatta",         // name
					"3674",                   // cityId
					"KOTA TANGERANG SELATAN", // cityName
					"360",                    // countryId
					time.Date(2024, 10, 31, 9, 10, 3, 359000000, time.UTC), // createdAt
					time.Date(2024, 11, 2, 16, 8, 7, 18000000, time.UTC),   // modifiedAt
					departureCreatedBy,  // createdBy
					&departureCreatedBy, // modifiedBy
					"INDONESIA",         // countryName
				),
				AirportID: 3,
			},
			Airline: helper.AirlineJSON{
				ID:          1,
				Code:        airlineCode,
				Logo:        airlineLogo.String,
				Name:        airlineName,
				CountryID:   "Tidak Ditemukan",
				CreatedAt:   airlineCreatedAt,
				CreatedBy:   "migration",
				ModifiedAt:  airlineUpdatedAt,
				ModifiedBy:  nil,
				CountryName: "Tidak Ditemukan",
			},
			AirlineID: 1,
		}

		// Get airline data for arrival
		err = airlineStmt.QueryRow(arrivalAirlineID).Scan(
			&airlineCode,
			&airlineLogo,
			&airlineName,
			&airlineCreatedAt,
			&airlineUpdatedAt,
		)
		if err != nil && err != sql.ErrNoRows {
			log.Printf("Error getting airline data: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		// Create arrival flight JSON
		arrivalFlight := helper.FlightJSON{
			To: helper.AirportWrapperJSON{
				Airport: helper.CreateAirportJSON(
					3,                        // id
					"SOE",                    // code
					"Soekarno Hatta",         // name
					"3674",                   // cityId
					"KOTA TANGERANG SELATAN", // cityName
					"360",                    // countryId
					time.Date(2024, 10, 31, 9, 10, 3, 359000000, time.UTC), // createdAt
					time.Date(2024, 11, 2, 16, 8, 7, 18000000, time.UTC),   // modifiedAt
					departureCreatedBy,  // createdBy
					&departureCreatedBy, // modifiedBy
					"INDONESIA",         // countryName
				),
				AirportID: 3,
			},
			From: helper.AirportWrapperJSON{
				Airport: helper.CreateAirportJSON(
					6,                              // id
					"JED",                          // code
					"Internasional King Abdulaziz", // name
					"0213",                         // cityId
					"JEDDAH",                       // cityName
					"682",                          // countryId
					time.Date(2024, 12, 28, 16, 35, 56, 423000000, time.UTC), // createdAt
					time.Date(2024, 12, 28, 16, 35, 56, 475415000, time.UTC), // modifiedAt
					departureCreatedBy, // createdBy
					nil,                // modifiedBy
					"JEDDAH",           // countryName
				),
				AirportID: 6,
			},
			Airline: helper.AirlineJSON{
				ID:          1,
				Code:        airlineCode,
				Logo:        airlineLogo.String,
				Name:        airlineName,
				CountryID:   "Tidak Ditemukan",
				CreatedAt:   airlineCreatedAt,
				CreatedBy:   "migration",
				ModifiedAt:  airlineUpdatedAt,
				ModifiedBy:  nil,
				CountryName: "Tidak Ditemukan",
			},
			AirlineID: 1,
		}

		// Convert to JSON
		departureJSON, err := json.Marshal(departureFlight)
		if err != nil {
			log.Printf("Error marshaling departure flight: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		arrivalJSON, err := json.Marshal(arrivalFlight)
		if err != nil {
			log.Printf("Error marshaling arrival flight: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		// Convert package type
		finalPackageType := "umrah"
		if packageType == "2" {
			finalPackageType = "hajj"
		}

		_, err = txInsertStmt.Exec(
			travelID,                 // organization_id
			organizationInstanceID,   // organization_instance_id
			finalPackageType,         // package_type
			image.String,             // thumbnail
			name,                     // title
			shareDesc.String,         // description
			termCondition.String,     // terms_condition
			facility.String,          // facility
			currency,                 // currency
			medinaHotelJSON,          // medina_hotel
			meccaHotelJSON,           // mecca_hotel
			departureJSON,            // departure
			arrivalJSON,              // arrival
			dpType,                   // dp_type
			int(dpAmount),            // dp_amount
			feeType,                  // fee_type
			int(feeAmount),           // fee_amount
			softDelete,               // deleted
			createdAt,                // created_at
			updatedAt,                // modified_at
			"migration",              // created_by
			nil,                      // modified_by
			organizationInstanceName, // organization_instance_name
			orgInstanceJSON,          // organization_instance
			slug.String,              // slug
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

	// Update progress bar untuk standardisasi nama kota
	bar.Finish()
	fmt.Printf("\n[2/3] Standardizing city names...\n")

	// Standardisasi nama kota Mekah/Mekkah
	updateMeccaResult, err := localUmrahDB.Exec(`
		UPDATE package
		SET mecca_hotel = jsonb_set(
			mecca_hotel,
			'{cityName}',
			'"MAKKAH"'
		)
		WHERE mecca_hotel->>'cityName' = 'Mekkah'
		OR mecca_hotel->>'cityName' = 'Mekah'
	`)
	if err != nil {
		log.Printf("Error updating Mecca city names: %v", err)
	}

	// Standardisasi nama kota Madinah
	updateMadinahResult, err := localUmrahDB.Exec(`
		UPDATE package
		SET medina_hotel = jsonb_set(
			medina_hotel,
			'{cityName}',
			'"MADINAH"'
		)
		WHERE medina_hotel->>'cityName' = 'Madinah'
	`)
	if err != nil {
		log.Printf("Error updating Madinah city names: %v", err)
	}

	// Get number of rows affected
	meccaRowsAffected, _ := updateMeccaResult.RowsAffected()
	madinahRowsAffected, _ := updateMadinahResult.RowsAffected()

	fmt.Printf("\n[3/3] City name standardization completed!\n")
	fmt.Printf("Standardized %d Mecca hotel records\n", meccaRowsAffected)
	fmt.Printf("Standardized %d Madinah hotel records\n", madinahRowsAffected)

	duration := time.Since(startTime)

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
