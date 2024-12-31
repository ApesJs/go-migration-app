package _package

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/ApesJs/go-migration-app/database"
	"github.com/ApesJs/go-migration-app/service/package/helper"
	"github.com/schollz/progressbar/v3"
	"log"
	"sort"
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
	orgInstanceStmt, err := helper.OrgInsStmt(devIdentityDB)
	if err != nil {
		log.Fatal("Error preparing organization instance statement:", err)
	}
	defer orgInstanceStmt.Close()

	// Statement untuk mendapatkan nama travel
	travelStmt, err := helper.TravelStmt(prodExistingUmrahDB)
	if err != nil {
		log.Fatal("Error preparing travel statement:", err)
	}
	defer travelStmt.Close()

	// Statement untuk mengecek hotel data
	hotelStmt, err := helper.HotelStmt(prodExistingUmrahDB)
	if err != nil {
		log.Fatal("Error preparing hotel check statement:", err)
	}
	defer hotelStmt.Close()

	// Statement untuk mengambil data airline
	airlineStmt, err := helper.AirlineStmt(prodExistingUmrahDB)
	if err != nil {
		log.Fatal("Error preparing airline statement:", err)
	}
	defer airlineStmt.Close()

	// Statement untuk insert ke tabel package
	insertPackageStmt, err := helper.InsertPackageStmt(localUmrahDB)
	if err != nil {
		log.Fatal("Error preparing package insert statement:", err)
	}
	defer insertPackageStmt.Close()

	// Statement untuk insert ke tabel package_variant
	insertVariantStmt, err := helper.InsertVariantStmt(localUmrahDB)
	if err != nil {
		log.Fatal("Error preparing variant insert statement:", err)
	}
	defer insertVariantStmt.Close()

	// Query untuk mengambil data package
	rows, err := helper.GetDataPackage(prodExistingUmrahDB)
	if err != nil {
		log.Fatal("Error querying source database:", err)
	}
	defer rows.Close()

	// Statement untuk mengambil data itinerary
	itineraryStmt, err := helper.GetPackageItineraryStmt(prodExistingUmrahDB)
	if err != nil {
		log.Fatal("Error preparing itinerary statement:", err)
	}
	defer itineraryStmt.Close()

	// Statement untuk insert itinerary
	insertItineraryStmt, err := helper.InsertItineraryStmt(localUmrahDB)
	if err != nil {
		log.Fatal("Error preparing itinerary insert statement:", err)
	}
	defer insertItineraryStmt.Close()

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
	txInsertItineraryStmt := tx.Stmt(insertItineraryStmt)

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

		// Process itinerary data
		itineraryRows, err := itineraryStmt.Query(id)
		if err != nil {
			log.Printf("Error querying itinerary data: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		var agendaItems []helper.AgendaItem
		dayCounter := 1

		// Temporary storage untuk grouping
		type TempActivity struct {
			Time     time.Time
			Activity string
			Location string
		}
		var activities []TempActivity

		// Collect semua activities dulu
		for itineraryRows.Next() {
			var (
				activityTime time.Time
				activity     string
				cityID       string
				createTime   time.Time
				cityName     string
			)

			err := itineraryRows.Scan(&activityTime, &activity, &cityID, &createTime, &cityName)
			if err != nil {
				log.Printf("Error scanning itinerary row: %v", err)
				continue
			}

			activities = append(activities, TempActivity{
				Time:     activityTime,
				Activity: activity,
				Location: cityName,
			})
		}
		itineraryRows.Close()

		// Sort activities berdasarkan waktu untuk satu hari
		sort.Slice(activities, func(i, j int) bool {
			return activities[i].Time.Before(activities[j].Time)
		})

		// Group activities per hari
		currentDay := time.Time{}
		var currentActivities []helper.Activity

		for i, act := range activities {
			if !act.Time.Truncate(24 * time.Hour).Equal(currentDay) {
				if len(currentActivities) > 0 {
					agendaItems = append(agendaItems, helper.AgendaItem{
						Title:      fmt.Sprintf("Hari Ke %d", dayCounter),
						Activities: currentActivities,
					})
					dayCounter++
				}
				currentDay = act.Time.Truncate(24 * time.Hour)
				currentActivities = []helper.Activity{}
			}

			currentActivities = append(currentActivities, helper.Activity{
				Time:     act.Time.Format("15:04"),
				Activity: act.Activity,
				Location: act.Location,
			})

			// Handle last group
			if i == len(activities)-1 {
				agendaItems = append(agendaItems, helper.AgendaItem{
					Title:      fmt.Sprintf("Hari Ke %d", dayCounter),
					Activities: currentActivities,
				})
			}
		}

		// Convert to JSON
		agendaJSON, err := json.Marshal(agendaItems)
		if err != nil {
			log.Printf("Error marshaling agenda: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		// Insert itinerary
		_, err = txInsertItineraryStmt.Exec(
			packageID,   // package_id
			agendaJSON,  // agenda
			createdAt,   // created_at
			updatedAt,   // modified_at
			"migration", // created_by
		)

		if err != nil {
			log.Printf("Error inserting itinerary: %v", err)
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
