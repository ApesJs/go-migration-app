package service

import (
	"database/sql"
	"fmt"
	"github.com/ApesJs/go-migration-app/database"
	"github.com/schollz/progressbar/v3"
	"log"
	"regexp"
	"strings"
	"time"
)

// createSlug membuat slug dari string yang diberikan
func createSlug(name string) string {
	// Mengkonversi ke lowercase
	slug := strings.ToLower(name)

	// Menghapus karakter khusus dan mengganti spasi dengan dash
	reg := regexp.MustCompile("[^a-z0-9]+")
	slug = reg.ReplaceAllString(slug, "-")

	// Menghapus dash di awal dan akhir string
	slug = strings.Trim(slug, "-")

	return slug
}

func TravelService() {
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
			id, name, slug, "desc", is_active, 
			soft_delete, created_at, updated_at
		FROM td_travel
	`)
	if err != nil {
		log.Fatal("Error querying source database:", err)
	}
	defer rows.Close()

	// Prepare statement untuk mengecek duplikasi (hanya berdasarkan id)
	checkStmt, err := targetDB.Prepare(`SELECT COUNT(*) FROM organization WHERE id = $1`)
	if err != nil {
		log.Fatal("Error preparing check statement:", err)
	}
	defer checkStmt.Close()

	// Prepare statement untuk insert
	insertStmt, err := targetDB.Prepare(`
		INSERT INTO organization (
			id, name, slug, description, thumbnail,
			is_active, deleted, created_at, modified_at,
			created_by, modified_by
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9,
			$10, $11
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
		duplicateItems   []string // Slice untuk menyimpan item yang duplikat
		generatedSlugs   []string // Slice untuk menyimpan item yang slugnya di-generate
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
		slug       sql.NullString
		desc       sql.NullString
		isActive   bool
		softDelete bool
		createdAt  time.Time
		updatedAt  time.Time
	)

	for rows.Next() {
		// Scan data dari source database
		err := rows.Scan(&id, &name, &slug, &desc, &isActive, &softDelete, &createdAt, &updatedAt)
		if err != nil {
			log.Printf("Error scanning row: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		// Cek apakah id sudah ada di database target
		var count int
		err = txCheckStmt.QueryRow(id).Scan(&count)
		if err != nil {
			log.Printf("Error checking for duplicates: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		// Jika id sudah ada, catat sebagai duplikat
		if count > 0 {
			duplicateItems = append(duplicateItems, fmt.Sprintf("%s (%s)", name, id))
			skipCount++
			bar.Add(1)
			continue
		}

		// Generate slug jika null
		finalSlug := slug.String
		if !slug.Valid || finalSlug == "" {
			finalSlug = createSlug(name)
			generatedSlugs = append(generatedSlugs, fmt.Sprintf("%s (%s) -> %s", name, id, finalSlug))
		}

		// Insert ke target database
		_, err = txInsertStmt.Exec(
			id,          // id
			name,        // name
			finalSlug,   // slug (menggunakan slug yang sudah di-handle)
			desc.String, // description
			nil,         // thumbnail (null)
			isActive,    // is_active
			softDelete,  // deleted
			createdAt,   // created_at
			updatedAt,   // modified_at
			"migration", // created_by
			nil,         // modified_by
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
