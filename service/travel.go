package service

import (
	"database/sql"
	"fmt"
	"github.com/schollz/progressbar/v3"
	"log"
	"github.com/ApesJs/go-migration-app/database"
	"time"
)

func TravelService() {
	//Panggil Koneksi Database
	sourceDB, targetDB := database.ConnectionDB()
	defer func(sourceDB *sql.DB) {
		err := sourceDB.Close()
		if err != nil {
			log.Fatal("Error at sourceDB in user.go line 18:", err)
		}
	}(sourceDB)

	defer func(targetDB *sql.DB) {
		err := targetDB.Close()
		if err != nil {
			log.Fatal("Error at targetDB in user.go line 25:", err)
		}
	}(targetDB)

	// Menghitung total records yang akan ditransfer
	var totalRows int
	err := sourceDB.QueryRow("SELECT COUNT(*) FROM td_user WHERE role = 'user'").Scan(&totalRows)
	if err != nil {
		log.Fatal("Error counting rows:", err)
	}

	fmt.Printf("Found %d records to transfer\n", totalRows)

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
	rows, err := sourceDB.Query("SELECT id, name, email, role, image, soft_delete, created_at, updated_at FROM td_user WHERE role = 'user'")
	if err != nil {
		log.Fatal("Error querying source database:", err)
	}
	defer rows.Close()

	// Prepare statement untuk mengecek duplikasi
	checkStmt, err := targetDB.Prepare(`SELECT COUNT(*) FROM "td_user" WHERE email = $1`)
	if err != nil {
		log.Fatal("Error preparing check statement:", err)
	}
	defer checkStmt.Close()

	// Prepare statement untuk insert
	insertStmt, err := targetDB.Prepare(`
		INSERT INTO "td_user" (
			id, name, username, email, role,
			is_active, email_verified,
			avatar, avatar_provider, provider,
			deleted, created_at, modified_at,
			created_by, modified_by
		) VALUES (
			$1, $2, $3, $4, $5,
			false, false,
			null, $6, null,
			$7, $8, $9,
			null, null
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
	)

	// Begin transaction
	tx, err := targetDB.Begin()
	if err != nil {
		log.Fatal("Error starting transaction:", err)
	}

	// Prepare statements dalam transaksi
	txCheckStmt := tx.Stmt(checkStmt)
	txInsertStmt := tx.Stmt(insertStmt)

	startTime := time.Now()

	// Memproses setiap baris data
	var (
		id         string
		name       string
		email      string
		role       string
		image      sql.NullString
		softDelete sql.NullBool
		createdAt  time.Time
		updatedAt  time.Time
	)

	for rows.Next() {
		// Scan data dari source database
		err := rows.Scan(&id, &name, &email, &role, &image, &softDelete, &createdAt, &updatedAt)
		if err != nil {
			log.Printf("Error scanning row: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		// Cek apakah email sudah ada di database target
		var count int
		err = txCheckStmt.QueryRow(email).Scan(&count)
		if err != nil {
			log.Printf("Error checking for duplicate email: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		// Jika email sudah ada, skip record ini
		if count > 0 {
			skipCount++
			bar.Add(1)
			continue
		}

		// Insert ke target database jika email belum ada
		_, err = txInsertStmt.Exec(
			id,         // id
			name,       // name
			email,      // username (dari email)
			email,      // email
			role,       // role
			image,      // avatar_provider (dari image)
			softDelete, // deleted (dari soft_delete)
			createdAt,  // created_at
			updatedAt,  // modified_at (dari updated_at)
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
}
