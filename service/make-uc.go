package service

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/ApesJs/go-migration-app/database"
	"github.com/schollz/progressbar/v3"
	"log"
	"time"
)

func MakeUCService() {
	// Koneksi Database
	devIdentityDB := database.ConnectionDevIdentityDB()
	defer devIdentityDB.Close()

	// Menghitung total records yang perlu dibuatkan credentials
	var totalRows int
	err := devIdentityDB.QueryRow(`
        SELECT COUNT(u.id)
        FROM "user" u
        LEFT JOIN user_credentials uc ON u.id = uc.id
        WHERE uc.id IS NULL AND u.deleted = false
    `).Scan(&totalRows)
	if err != nil {
		log.Fatal("Error counting rows:", err)
	}

	fmt.Printf("Found %d users without credentials\n", totalRows)

	// Jika tidak ada data yang perlu diproses
	if totalRows == 0 {
		fmt.Println("No users need credentials. Exiting...")
		return
	}

	// Membuat progress bar
	bar := progressbar.NewOptions(totalRows,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription("[cyan][1/1][reset] Generating credentials..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
	)

	// Prepare statement untuk insert
	insertStmt, err := devIdentityDB.Prepare(`
        INSERT INTO user_credentials (id, salt, hashed_password)
        VALUES ($1, $2, $3)
    `)
	if err != nil {
		log.Fatal("Error preparing insert statement:", err)
	}
	defer insertStmt.Close()

	// Begin transaction
	tx, err := devIdentityDB.Begin()
	if err != nil {
		log.Fatal("Error starting transaction:", err)
	}

	// Prepare statement dalam transaksi
	txInsertStmt := tx.Stmt(insertStmt)

	// Variabel untuk statistik
	var (
		successCount int
		errorCount   int
	)

	startTime := time.Now()

	// Query untuk mendapatkan user yang belum memiliki credentials
	rows, err := devIdentityDB.Query(`
        SELECT u.id 
        FROM "user" u
        LEFT JOIN user_credentials uc ON u.id = uc.id
        WHERE uc.id IS NULL AND u.deleted = false
    `)
	if err != nil {
		log.Fatal("Error querying users:", err)
	}
	defer rows.Close()

	// Proses setiap user
	for rows.Next() {
		var userID string
		err := rows.Scan(&userID)
		if err != nil {
			log.Printf("Error scanning user ID: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		// Generate salt (16 bytes = 32 chars hex)
		salt := make([]byte, 16)
		if _, err := rand.Read(salt); err != nil {
			log.Printf("Error generating salt: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}
		saltHex := hex.EncodeToString(salt)

		// Generate hashed password (32 bytes = 64 chars hex)
		hashedPw := make([]byte, 32)
		if _, err := rand.Read(hashedPw); err != nil {
			log.Printf("Error generating hashed password: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}
		hashedPwHex := hex.EncodeToString(hashedPw)

		// Insert credentials
		_, err = txInsertStmt.Exec(userID, saltHex, hashedPwHex)
		if err != nil {
			log.Printf("Error inserting credentials for user %s: %v", userID, err)
			errorCount++
			bar.Add(1)
			continue
		}

		successCount++
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
	fmt.Printf("\nCredentials Generation Summary:\n")
	fmt.Printf("-----------------------------\n")
	fmt.Printf("Total users processed: %d\n", totalRows)
	fmt.Printf("Successfully generated: %d\n", successCount)
	fmt.Printf("Failed generations: %d\n", errorCount)
	fmt.Printf("Duration: %s\n", duration.Round(time.Second))
	fmt.Printf("Average speed: %.2f records/second\n", float64(successCount)/duration.Seconds())
}
