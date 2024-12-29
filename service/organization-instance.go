package service

import (
	"database/sql"
	"fmt"
	"github.com/ApesJs/go-migration-app/database"
	"github.com/schollz/progressbar/v3"
	"log"
	"time"
)

func OrganizationInstanceService() {
	// Panggil Koneksi Database
	sourceDB, targetDB := database.ConnectionDB()
	defer sourceDB.Close()
	defer targetDB.Close()

	// Menghitung total records yang akan ditransfer
	var totalRows int
	err := sourceDB.QueryRow("SELECT COUNT(*) FROM td_travel").Scan(&totalRows)
	if err != nil {
		log.Fatal("Error counting rows:", err)
	}

	fmt.Printf("Found %d total records to transfer\n", totalRows)

	// Membuat progress bar
	bar := progressbar.NewOptions(totalRows,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription("[cyan][1/2][reset] Transferring data..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
	)

	// Mengambil data dari database sumber
	rows, err := sourceDB.Query(`
		SELECT 
			id, name, address, is_active, created_at, updated_at,
			image, phone, rda_id, xendit_channel, xendit_account_number,
			xendit_account_name, pic_name, pic_phone, email, soft_delete,
			tagline, action_profile, action_package, own_guide,
			fee_type, fee_amount, ppiu, pihk, is_consultation,
			city_id, "desc"
		FROM td_travel
	`)
	if err != nil {
		log.Fatal("Error querying source database:", err)
	}
	defer rows.Close()

	// Prepare statement untuk mengecek duplikasi (berdasarkan email karena unique)
	checkStmt, err := targetDB.Prepare(`SELECT COUNT(*) FROM organization_instance WHERE email = $1`)
	if err != nil {
		log.Fatal("Error preparing check statement:", err)
	}
	defer checkStmt.Close()

	// Prepare statement untuk mengecek keberadaan organization
	checkOrgStmt, err := targetDB.Prepare(`SELECT COUNT(*) FROM organization WHERE id = $1`)
	if err != nil {
		log.Fatal("Error preparing check organization statement:", err)
	}
	defer checkOrgStmt.Close()

	// Prepare statement untuk insert
	insertStmt, err := targetDB.Prepare(`
		INSERT INTO organization_instance (
			organization_id, type, name, slug, address,
			country_id, province_id, city_id, is_active,
			legal_information, created_at, modified_at,
			created_by, thumbnail, phone_number,
			bdm_id, bank_channel, bank_account_number,
			bank_account_name, pic_name, pic_phone,
			email, deleted, tagline,
			action_profile, action_package, own_guide,
			fee_type, fee_amount, ppiu,
			pihk, is_consultation, description
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9,
			$10, $11, $12,
			$13, $14, $15,
			$16, $17, $18,
			$19, $20, $21,
			$22, $23, $24,
			$25, $26, $27,
			$28, $29, $30,
			$31, $32, $33
		)
	`)
	if err != nil {
		log.Fatal("Error preparing insert statement:", err)
	}
	defer insertStmt.Close()

	// Variabel untuk statistik
	var (
		transferredCount int
		errorCount       int
		skipCount        int
		duplicateItems   []string
		generatedSlugs   []string
	)

	// Begin transaction
	tx, err := targetDB.Begin()
	if err != nil {
		log.Fatal("Error starting transaction:", err)
	}

	// Prepare statements dalam transaksi
	txCheckStmt := tx.Stmt(checkStmt)
	txCheckOrgStmt := tx.Stmt(checkOrgStmt)
	txInsertStmt := tx.Stmt(insertStmt)

	startTime := time.Now()

	// Memproses setiap baris data
	var (
		id              string
		name            string
		address         sql.NullString
		isActive        bool
		createdAt       time.Time
		updatedAt       time.Time
		image           sql.NullString
		phone           sql.NullString
		rdaID           sql.NullString
		xenditChannel   sql.NullString
		xenditAccNumber sql.NullString
		xenditAccName   sql.NullString
		picName         sql.NullString
		picPhone        sql.NullString
		email           sql.NullString
		softDelete      bool
		tagline         sql.NullString
		actionProfile   bool
		actionPackage   bool
		ownGuide        bool
		feeType         sql.NullString
		feeAmount       float64
		ppiu            sql.NullString
		pihk            sql.NullString
		isConsultation  bool
		cityID          sql.NullString
		description     sql.NullString
	)

	for rows.Next() {
		// Scan data dari source database
		err := rows.Scan(
			&id, &name, &address, &isActive, &createdAt, &updatedAt,
			&image, &phone, &rdaID, &xenditChannel, &xenditAccNumber,
			&xenditAccName, &picName, &picPhone, &email, &softDelete,
			&tagline, &actionProfile, &actionPackage, &ownGuide,
			&feeType, &feeAmount, &ppiu, &pihk, &isConsultation,
			&cityID, &description,
		)
		if err != nil {
			log.Printf("Error scanning row: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		// Cek duplikasi berdasarkan email
		if email.Valid && email.String != "" {
			var count int
			err = txCheckStmt.QueryRow(email.String).Scan(&count)
			if err != nil {
				log.Printf("Error checking for duplicates: %v", err)
				errorCount++
				bar.Add(1)
				continue
			}

			if count > 0 {
				duplicateItems = append(duplicateItems, fmt.Sprintf("%s (%s)", name, email.String))
				skipCount++
				bar.Add(1)
				continue
			}
		} else {
			// Jika email NULL atau empty string, generate unique identifier
			timestamp := time.Now().UnixNano()
			email.String = fmt.Sprintf("no-email-%s-%d@placeholder.com", id, timestamp)
			log.Printf("Generated placeholder email for organization %s: %s", name, email.String)
		}

		// Cek keberadaan organization_id
		var organizationID interface{}
		if id != "" {
			var count int
			err = txCheckOrgStmt.QueryRow(id).Scan(&count)
			if err != nil {
				log.Printf("Error checking organization: %v", err)
				organizationID = nil
			} else if count > 0 {
				organizationID = id
			} else {
				// ID tidak ditemukan di tabel organizations
				organizationID = "d0ac7aad-54ac-41f1-ba1a-a9070c3f464c"
				log.Printf("Organization ID %s not found in organizations table, setting to NULL", id)
			}
		} else {
			organizationID = nil
		}

		// Handle bdm_id
		var bdmID interface{}
		if rdaID.Valid {
			bdmID = rdaID.String
		} else {
			bdmID = nil
		}

		// Handle city_id
		//var finalCityID interface{}
		//if cityID.Valid {
		//	finalCityID = cityID.String
		//} else {
		//	finalCityID = "9999" // Default value jika NULL
		//}

		// Generate slug dari nama
		slug := createSlug(name)
		generatedSlugs = append(generatedSlugs, fmt.Sprintf("%s -> %s", name, slug))

		// Insert ke target database
		_, err = txInsertStmt.Exec(
			organizationID, // organization_id
			"travel",       // type
			name,           // name
			slug,           // slug
			address.String, // address
			"360",          // country_id (default)
			"31",
			"3173", // province_id (default)
			//finalCityID,               // city_id (dari source)
			isActive,                  // is_active
			`{"status": "belum ada"}`, // legal_information
			createdAt,                 // created_at
			updatedAt,                 // modified_at
			"migration",               // created_by
			image.String,              // thumbnail
			phone.String,              // phone_number
			bdmID,                     // bdm_id
			xenditChannel.String,      // bank_channel
			xenditAccNumber.String,    // bank_account_number
			xenditAccName.String,      // bank_account_name
			picName.String,            // pic_name
			picPhone.String,           // pic_phone
			email.String,              // email
			softDelete,                // deleted
			tagline.String,            // tagline
			actionProfile,             // action_profile
			actionPackage,             // action_package
			ownGuide,                  // own_guide
			feeType.String,            // fee_type
			feeAmount,                 // fee_amount
			ppiu.String,               // ppiu
			pihk.String,               // pihk
			isConsultation,            // is_consultation
			description,
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
	fmt.Printf("Skipped (duplicates): %d\n", skipCount)
	fmt.Printf("Failed transfers: %d\n", errorCount)
	fmt.Printf("Duration: %s\n", duration.Round(time.Second))
	fmt.Printf("Average speed: %.2f records/second\n", float64(transferredCount)/duration.Seconds())

	// Menampilkan list item duplikat
	if len(duplicateItems) > 0 {
		fmt.Printf("\nDuplicate Items:\n")
		fmt.Printf("---------------\n")
		for i, item := range duplicateItems {
			fmt.Printf("%d. %s\n", i+1, item)
		}
	}

	// Menampilkan list item yang slugnya di-generate
	if len(generatedSlugs) > 0 {
		fmt.Printf("\nGenerated Slugs:\n")
		fmt.Printf("---------------\n")
		for i, item := range generatedSlugs {
			fmt.Printf("%d. %s\n", i+1, item)
		}
	}
}
