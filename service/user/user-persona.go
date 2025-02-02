package user

import (
	"database/sql"
	"fmt"
	"github.com/ApesJs/go-migration-app/database"
	"github.com/schollz/progressbar/v3"
	"log"
	"time"
)

type DuplicatePhoneInfo struct {
	UserID      string
	PhoneNumber string
}

func UserPersonaService() {
	prodExistingUmrahDB := database.ConnectionProdExistingUmrahDB()
	defer prodExistingUmrahDB.Close()

	prodIdentityDB := database.ConnectionProdIdentityDB()
	defer prodIdentityDB.Close()

	//devIdentitylDB := database.ConnectionDevIdentityDB()
	//defer devIdentityDB.Close()

	//localIdentityDB := database.ConnectionLocalIdentityDB()
	//defer localIdentityDB.Close()

	fmt.Println("Memulai proses transfer data persona user...")

	alterTableQueries := []string{
		`ALTER TABLE "user_persona" ADD COLUMN IF NOT EXISTS address TEXT`,
		`ALTER TABLE "user_persona" ADD COLUMN IF NOT EXISTS job VARCHAR(255)`,
		`ALTER TABLE "user_persona" ADD COLUMN IF NOT EXISTS dob TIMESTAMP WITH TIME ZONE`,
	}

	for _, query := range alterTableQueries {
		_, err := prodIdentityDB.Exec(query)
		if err != nil {
			log.Fatal("Error saat menambahkan kolom:", err)
		}
	}

	var totalRows int
	err := prodIdentityDB.QueryRow(`SELECT COUNT(*) FROM "user"`).Scan(&totalRows)
	if err != nil {
		log.Fatal("Error saat menghitung total rows:", err)
	}

	fmt.Printf("Total user yang akan diproses: %d\n", totalRows)

	bar := progressbar.NewOptions(totalRows,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription("[cyan][1/1][reset] Memproses data persona..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
	)

	// Query untuk mengecek nomor telepon yang sudah ada
	checkPhoneStmt, err := prodIdentityDB.Prepare(`
		SELECT id FROM "user_persona" WHERE phone_number = $1 LIMIT 1
	`)
	if err != nil {
		log.Fatal("Error preparing check phone statement:", err)
	}
	defer checkPhoneStmt.Close()

	checkStmt, err := prodIdentityDB.Prepare(`
		SELECT COUNT(*) FROM "user_persona" WHERE id = $1
	`)
	if err != nil {
		log.Fatal("Error preparing check statement:", err)
	}
	defer checkStmt.Close()

	insertStmt, err := prodIdentityDB.Prepare(`
		INSERT INTO "user_persona" (
			id, phone_number, address, gender,
			job, born, dob
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
	`)
	if err != nil {
		log.Fatal("Error preparing insert statement:", err)
	}
	defer insertStmt.Close()

	updateStmt, err := prodIdentityDB.Prepare(`
		UPDATE "user_persona" SET
			phone_number = $2,
			address = $3,
			gender = $4,
			job = $5,
			born = $6,
			dob = $7
		WHERE id = $1
	`)
	if err != nil {
		log.Fatal("Error preparing update statement:", err)
	}
	defer updateStmt.Close()

	var (
		insertCount     int
		updateCount     int
		errorCount      int
		skippedCount    int
		startTime       = time.Now()
		processedRows   = 0
		duplicatePhones = make([]DuplicatePhoneInfo, 0)
	)

	// Map untuk melacak nomor telepon yang sudah digunakan
	usedPhoneNumbers := make(map[string]string)

	// Pertama, ambil semua nomor telepon yang sudah ada di database target
	existingPhones, err := prodIdentityDB.Query(`
		SELECT id, phone_number FROM "user_persona" WHERE phone_number IS NOT NULL
	`)
	if err != nil {
		log.Fatal("Error querying existing phone numbers:", err)
	}
	for existingPhones.Next() {
		var id, phone string
		if err := existingPhones.Scan(&id, &phone); err != nil {
			log.Fatal("Error scanning existing phone numbers:", err)
		}
		if phone != "" {
			usedPhoneNumbers[phone] = id
		}
	}
	existingPhones.Close()

	rows, err := prodIdentityDB.Query(`SELECT id FROM "user"`)
	if err != nil {
		log.Fatal("Error querying user data:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var userID string
		err := rows.Scan(&userID)
		if err != nil {
			log.Printf("Error scanning user ID: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		var (
			phone   sql.NullString
			address sql.NullString
			gender  sql.NullString
			job     sql.NullString
			pob     sql.NullString
			dob     sql.NullTime
		)

		err = prodExistingUmrahDB.QueryRow(`
			SELECT phone, address, gender, job, pob, dob
			FROM td_user WHERE id = $1
		`, userID).Scan(&phone, &address, &gender, &job, &pob, &dob)

		if err == sql.ErrNoRows {
			skippedCount++
			bar.Add(1)
			continue
		} else if err != nil {
			log.Printf("Error querying source data for user %s: %v", userID, err)
			errorCount++
			bar.Add(1)
			continue
		}

		var exists int
		err = checkStmt.QueryRow(userID).Scan(&exists)
		if err != nil {
			log.Printf("Error checking existing record: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		// Cek apakah phone number valid dan sudah digunakan
		if phone.Valid && phone.String != "" {
			if existingID, exists := usedPhoneNumbers[phone.String]; exists && existingID != userID {
				// Catat nomor telepon yang duplikat
				duplicatePhones = append(duplicatePhones, DuplicatePhoneInfo{
					UserID:      userID,
					PhoneNumber: phone.String,
				})
				// Set phone number menjadi NULL
				phone.Valid = false
				phone.String = ""
			} else if !exists {
				// Jika nomor belum digunakan, tambahkan ke map
				usedPhoneNumbers[phone.String] = userID
			}
		}

		if gender.Valid && len(gender.String) > 16 {
			gender.String = gender.String[:16]
		}

		if phone.Valid && len(phone.String) > 16 {
			phone.String = phone.String[:16]
		}

		if pob.Valid && len(pob.String) > 10 {
			pob.String = pob.String[:10]
		}

		var phoneValue interface{}
		if phone.Valid {
			phoneValue = phone.String
		} else {
			phoneValue = nil
		}

		if exists > 0 {
			_, err = updateStmt.Exec(
				userID,
				phoneValue,
				address.String,
				gender.String,
				job.String,
				pob.String,
				dob.Time,
			)
			if err != nil {
				log.Printf("Error updating record for user %s: %v", userID, err)
				errorCount++
			} else {
				updateCount++
			}
		} else {
			_, err = insertStmt.Exec(
				userID,
				phoneValue,
				address.String,
				gender.String,
				job.String,
				pob.String,
				dob.Time,
			)
			if err != nil {
				log.Printf("Error inserting record for user %s: %v", userID, err)
				errorCount++
			} else {
				insertCount++
			}
		}

		processedRows++
		bar.Add(1)
	}

	duration := time.Since(startTime)

	bar.Finish()
	fmt.Printf("\nTransfer data persona selesai!\n")
	fmt.Printf("\nRingkasan Hasil:\n")
	fmt.Printf("---------------\n")
	fmt.Printf("Total records diproses: %d\n", processedRows)
	fmt.Printf("Records berhasil ditambahkan: %d\n", insertCount)
	fmt.Printf("Records berhasil diperbarui: %d\n", updateCount)
	fmt.Printf("Records dilewati: %d\n", skippedCount)
	fmt.Printf("Records gagal: %d\n", errorCount)
	fmt.Printf("Durasi proses: %s\n", duration.Round(time.Second))
	fmt.Printf("Kecepatan rata-rata: %.2f records/detik\n", float64(processedRows)/duration.Seconds())

	if len(duplicatePhones) > 0 {
		fmt.Printf("\nDaftar Nomor Telepon Duplikat:\n")
		fmt.Printf("--------------------------------\n")
		fmt.Printf("%-36s | %s\n", "User ID", "Nomor Telepon")
		fmt.Printf("--------------------------------\n")
		for _, dup := range duplicatePhones {
			fmt.Printf("%-36s | %s\n", dup.UserID, dup.PhoneNumber)
		}
		fmt.Printf("\nTotal nomor telepon duplikat: %d\n", len(duplicatePhones))
	}
}
