package travel

import (
	"fmt"
	"github.com/ApesJs/go-migration-app/database"
	"github.com/schollz/progressbar/v3"
	"log"
	"time"
)

func OrganizationUserService() {
	// Panggil Koneksi Database
	prodExistingUmrahDB := database.ConnectionProdExistingUmrahDB()
	devIdentityDB := database.ConnectionDevIdentityDB()
	defer prodExistingUmrahDB.Close()
	defer devIdentityDB.Close()

	// Menghitung total records yang akan ditransfer
	var totalRows int
	err := prodExistingUmrahDB.QueryRow("SELECT COUNT(*) FROM td_travel_user").Scan(&totalRows)
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
	rows, err := prodExistingUmrahDB.Query(`
		SELECT 
			travel_id, user_id, role
		FROM td_travel_user
	`)
	if err != nil {
		log.Fatal("Error querying source database:", err)
	}
	defer rows.Close()

	// Prepare statement untuk mengecek keberadaan organization
	checkOrgStmt, err := devIdentityDB.Prepare(`SELECT COUNT(*) FROM organization WHERE id = $1`)
	if err != nil {
		log.Fatal("Error preparing check organization statement:", err)
	}
	defer checkOrgStmt.Close()

	// Prepare statement untuk mengecek keberadaan user
	checkUserStmt, err := devIdentityDB.Prepare(`SELECT COUNT(*) FROM "user" WHERE id = $1`)
	if err != nil {
		log.Fatal("Error preparing check user statement:", err)
	}
	defer checkUserStmt.Close()

	// Prepare statement untuk mengecek duplikasi
	checkDuplicateStmt, err := devIdentityDB.Prepare(`
		SELECT COUNT(*) FROM organization_user 
		WHERE organization_id = $1 AND user_id = $2
	`)
	if err != nil {
		log.Fatal("Error preparing check duplicate statement:", err)
	}
	defer checkDuplicateStmt.Close()

	// Prepare statement untuk insert
	insertStmt, err := devIdentityDB.Prepare(`
		INSERT INTO organization_user (
			organization_id, user_id, role,
			created_at, modified_at,
			created_by, modified_by
		) VALUES (
			$1, $2, $3,
			CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
			'migration', NULL
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
	)

	// Begin transaction
	tx, err := devIdentityDB.Begin()
	if err != nil {
		log.Fatal("Error starting transaction:", err)
	}

	// Prepare statements dalam transaksi
	txCheckOrgStmt := tx.Stmt(checkOrgStmt)
	txCheckUserStmt := tx.Stmt(checkUserStmt)
	txCheckDuplicateStmt := tx.Stmt(checkDuplicateStmt)
	txInsertStmt := tx.Stmt(insertStmt)

	startTime := time.Now()

	// Memproses setiap baris data
	var (
		travelID string
		userID   string
		role     string
	)

	for rows.Next() {
		// Scan data dari source database
		err := rows.Scan(&travelID, &userID, &role)
		if err != nil {
			log.Printf("Error scanning row: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		// Cek keberadaan organization_id
		var organizationID interface{}
		if travelID != "" {
			var count int
			err = txCheckOrgStmt.QueryRow(travelID).Scan(&count)
			if err != nil {
				log.Printf("Error checking organization: %v", err)
				errorCount++
				bar.Add(1)
				continue
			}
			if count > 0 {
				organizationID = travelID
			} else {
				organizationID = "d0ac7aad-54ac-41f1-ba1a-a9070c3f464c"
				log.Printf("Organization ID %s not found in organizations table, setting to default ID", travelID)
			}
		} else {
			errorCount++
			bar.Add(1)
			continue
		}

		// Cek keberadaan user_id
		var count int
		err = txCheckUserStmt.QueryRow(userID).Scan(&count)
		if err != nil {
			log.Printf("Error checking user: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}
		if count == 0 {
			log.Printf("User ID %s not found in user table, skipping", userID)
			errorCount++
			bar.Add(1)
			continue
		}

		// Cek duplikasi
		err = txCheckDuplicateStmt.QueryRow(organizationID, userID).Scan(&count)
		if err != nil {
			log.Printf("Error checking for duplicates: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		if count > 0 {
			duplicateItems = append(duplicateItems, fmt.Sprintf("org: %s, user: %s", organizationID, userID))
			skipCount++
			bar.Add(1)
			continue
		}

		// Insert ke target database
		_, err = txInsertStmt.Exec(
			organizationID, // organization_id
			userID,         // user_id
			role,           // role
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
}
