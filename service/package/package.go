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
	totalRows, err := helper.TotalRows(prodExistingUmrahDB)
	if err != nil {
		log.Fatal("Error counting rows:", err)
	}

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
		log.Fatal("Error preparing travel statement:", err)
	}
	defer travelStmt.Close()

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
	insertPackageStmt, err := localUmrahDB.Prepare(`
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
		) RETURNING id
	`)
	if err != nil {
		log.Fatal("Error preparing package insert statement:", err)
	}
	defer insertPackageStmt.Close()

	// Statement untuk insert ke tabel package_variant
	insertVariantStmt, err := localUmrahDB.Prepare(`
		INSERT INTO package_variant (
			package_id, thumbnail, name, departure_date, arrival_date,
			original_price_double, original_price_triple, original_price_quad,
			price_double, price_triple, price_quad,
			released_at, published, created_at, modified_at, created_by
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
		)
	`)
	if err != nil {
		log.Fatal("Error preparing variant insert statement:", err)
	}
	defer insertVariantStmt.Close()

	// Query untuk mengambil data package
	rows, err := prodExistingUmrahDB.Query(`
		SELECT id, travel_id, departure_airline_id, arrival_airline_id,
			   name, slug, image, type, share_desc, term_condition,
			   facility, currency, dp_type, dp_amount, fee_type,
			   fee_amount, soft_delete, created_at, updated_at,
			   departure_date, arrival_date, price_double, price_triple,
			   price_quad
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
		variantCount        int
		missingOrgInstances []helper.MissingOrgInstance
	)

	// Begin transaction
	tx, err := localUmrahDB.Begin()
	if err != nil {
		log.Fatal("Error starting transaction:", err)
	}

	// Prepare statements dalam transaksi
	txInsertPackageStmt := tx.Stmt(insertPackageStmt)
	txInsertVariantStmt := tx.Stmt(insertVariantStmt)

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
			arrivalDate        time.Time
			priceDouble        float64
			priceTriple        float64
			priceQuad          float64
		)

		// Scan data dari source database
		err := rows.Scan(
			&id, &travelID, &departureAirlineID, &arrivalAirlineID,
			&name, &slug, &image, &packageType, &shareDesc, &termCondition,
			&facility, &currency, &dpType, &dpAmount, &feeType,
			&feeAmount, &softDelete, &createdAt, &updatedAt,
			&departureDate, &arrivalDate, &priceDouble, &priceTriple,
			&priceQuad,
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
			organizationInstanceName string
		)
		err = orgInstanceStmt.QueryRow(travelID).Scan(&organizationInstanceID, &organizationInstanceName)
		if err != nil {
			if err == sql.ErrNoRows {
				organizationInstanceID = 9999
				organizationInstanceName = "Nama Travel Tidak di Temukan"

				var travelName string
				err := travelStmt.QueryRow(travelID).Scan(&travelName)
				if err != nil {
					travelName = "Unknown Travel Name"
					if err != sql.ErrNoRows {
						log.Printf("Error getting travel name for ID %s: %v", travelID, err)
					}
				}

				missingOrgInstances = append(missingOrgInstances, helper.MissingOrgInstance{
					TravelID:   travelID,
					TravelName: travelName,
				})
				log.Printf("No organization_instance found for travel_id %s (%s), using default value 9999", travelID, travelName)
			} else {
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
			orgInstanceJSON = []byte(`{"status": "belum ada"}`)
		}

		// Process hotel data
		hotelRows, err := hotelStmt.Query(id)
		if err != nil {
			log.Printf("Error querying hotel data: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

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

		var medinaHotel, meccaHotel *helper.HotelJSON
		if len(medinaHotels) > 0 {
			medinaHotel = medinaHotels[0]
		}
		if len(meccaHotels) > 0 {
			meccaHotel = meccaHotels[0]
		}

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

		// Get airline data
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

		// Create flight JSONs
		departureFlight, err := helper.CreateDepartureJSON(airlineCode, airlineLogo, airlineName, airlineCreatedAt, airlineUpdatedAt, airlineStmt, arrivalAirlineID)
		if err != nil && err != sql.ErrNoRows {
			log.Printf("Error creating departure flight: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		arrivalFlight := helper.CreateArrivalJSON(airlineCode, airlineLogo, airlineName, airlineCreatedAt, airlineUpdatedAt)

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

		// Insert package and get ID
		var packageID int
		err = txInsertPackageStmt.QueryRow(
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
		).Scan(&packageID)

		if err != nil {
			log.Printf("Error inserting package: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		// Insert package_variant
		_, err = txInsertVariantStmt.Exec(
			packageID,                          // package_id
			image.String,                       // thumbnail
			name,                               // name
			departureDate.Format("2006-01-02"), // departure_date
			arrivalDate.Format("2006-01-02"),   // arrival_date
			int64(priceDouble),                 // original_price_double
			int64(priceTriple),                 // original_price_triple
			int64(priceQuad),                   // original_price_quad
			int64(priceDouble),                 // price_double
			int64(priceTriple),                 // price_triple
			int64(priceQuad),                   // price_quad
			updatedAt,                          // released_at
			true,                               // published
			createdAt,                          // created_at
			updatedAt,                          // modified_at
			"migration",                        // created_by
		)

		if err != nil {
			log.Printf("Error inserting package variant: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		transferredCount++
		variantCount++
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
	fmt.Printf("Successfully transferred packages: %d\n", transferredCount)
	fmt.Printf("Successfully transferred variants: %d\n", variantCount)
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
