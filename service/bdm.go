package service

import (
	"database/sql"
	"fmt"
	"github.com/ApesJs/go-migration-app/database"
	"github.com/schollz/progressbar/v3"
	"log"
	"time"
)

func BDMService() {
	//Panggil Koneksi Database
	prodExistingUmrahDB := database.ConnectionProdExistingUmrahDB()
	devIdentityDB := database.ConnectionDevIdentityDB()
	defer prodExistingUmrahDB.Close()
	defer devIdentityDB.Close()

	// Konstanta untuk role
	const (
		roleName = "BDM"
		roleSlug = "bdm" // slug harus lowercase
	)

	// Pertama, periksa apakah role 'bdm' sudah ada di tabel role
	var roleExists bool
	err := devIdentityDB.QueryRow(`SELECT EXISTS(SELECT 1 FROM "role" WHERE slug = $1)`, roleSlug).Scan(&roleExists)
	if err != nil {
		log.Fatal("Error checking role existence:", err)
	}

	// Jika role belum ada, insert role terlebih dahulu
	if !roleExists {
		_, err = devIdentityDB.Exec(`INSERT INTO "role" (name, slug) 
							   VALUES ($1, $2)`,
			roleName, roleSlug)
		if err != nil {
			log.Fatal("Error inserting role:", err)
		}
		fmt.Printf("Created role with name '%s' and slug '%s'\n", roleName, roleSlug)
	}

	// Menghitung total records yang akan ditransfer
	var totalRows int
	err = prodExistingUmrahDB.QueryRow("SELECT COUNT(*) FROM tr_rda").Scan(&totalRows)
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
	rows, err := prodExistingUmrahDB.Query("SELECT id, name, email, phone, created_at, updated_at FROM tr_rda")
	if err != nil {
		log.Fatal("Error querying source database:", err)
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			log.Fatal("Error when closing connection to source database:", err)
		}
	}(rows)

	// Prepare statement untuk mengecek duplikasi
	checkStmt, err := devIdentityDB.Prepare(`SELECT COUNT(*) FROM "user" WHERE email = $1`)
	if err != nil {
		log.Fatal("Error preparing check statement:", err)
	}
	defer func(checkStmt *sql.Stmt) {
		err := checkStmt.Close()
		if err != nil {
			log.Fatal("Error when closing connection to checkStmt:", err)
		}
	}(checkStmt)

	// Prepare statement untuk insert dengan role slug
	insertStmt, err := devIdentityDB.Prepare(`
		INSERT INTO "user" (
			id, name, username, email, role,
			is_active, email_verified,
			avatar, avatar_provider, provider,
			deleted, created_at, modified_at,
			created_by, modified_by
		) VALUES (
			$1, $2, $3, $4, $5,
			true, false,
			null, null, null,
			false, $6, $7,
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
	tx, err := devIdentityDB.Begin()
	if err != nil {
		log.Fatal("Error starting transaction:", err)
	}

	// Prepare statements dalam transaksi
	txCheckStmt := tx.Stmt(checkStmt)
	txInsertStmt := tx.Stmt(insertStmt)

	startTime := time.Now()

	// Memproses setiap baris data
	var (
		id        string
		name      string
		email     string
		phone     string
		createdAt time.Time
		updatedAt time.Time
	)

	for rows.Next() {
		// Scan data dari source database
		err := rows.Scan(&id, &name, &email, &phone, &createdAt, &updatedAt)
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

		// Insert ke target database dengan role slug
		_, err = txInsertStmt.Exec(
			id,        // id
			name,      // name
			email,     // username (dari email)
			email,     // email
			roleSlug,  // role (menggunakan slug)
			createdAt, // created_at
			updatedAt, // modified_at
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
