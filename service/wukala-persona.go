package service

import (
	"database/sql"
	"fmt"
	"github.com/ApesJs/go-migration-app/database"
	"github.com/schollz/progressbar/v3"
	"log"
	"time"
)

type DuplicatePhoneInfoWukala struct {
	UserID      string
	PhoneNumber string
}

func WukalaPersonaService() {
	prodExistingUmrahDB := database.ConnectionProdExistingUmrahDB()
	devIdentityDB := database.ConnectionDevIdentityDB()
	defer prodExistingUmrahDB.Close()
	defer devIdentityDB.Close()

	fmt.Println("Memulai proses transfer data persona wukala...")

	// Add new columns to user-persona table
	alterTableQueries := []string{
		`ALTER TABLE "user-persona" ADD COLUMN IF NOT EXISTS travel_id UUID`,
		`ALTER TABLE "user-persona" ADD COLUMN IF NOT EXISTS "desc" TEXT`,
		`ALTER TABLE "user-persona" ADD COLUMN IF NOT EXISTS code VARCHAR(50)`,
		`ALTER TABLE "user-persona" ADD COLUMN IF NOT EXISTS fee DOUBLE PRECISION`,
		`ALTER TABLE "user-persona" ADD COLUMN IF NOT EXISTS web_visit INTEGER`,
		`ALTER TABLE "user-persona" ADD COLUMN IF NOT EXISTS activated_at TIMESTAMP WITH TIME ZONE`,
		`ALTER TABLE "user-persona" ADD COLUMN IF NOT EXISTS discount DOUBLE PRECISION`,
		`ALTER TABLE "user-persona" ADD COLUMN IF NOT EXISTS parent_id UUID`,
		`ALTER TABLE "user-persona" ADD COLUMN IF NOT EXISTS fee_type VARCHAR(20)`,
		`ALTER TABLE "user-persona" ADD COLUMN IF NOT EXISTS discount_type VARCHAR(20)`,
		`ALTER TABLE "user-persona" ADD COLUMN IF NOT EXISTS bdm_user_id UUID`,
		`ALTER TABLE "user-persona" ADD COLUMN IF NOT EXISTS alias VARCHAR(50)`,
		`ALTER TABLE "user-persona" ADD COLUMN IF NOT EXISTS nik VARCHAR(20)`,
		`ALTER TABLE "user-persona" ADD COLUMN IF NOT EXISTS instagram VARCHAR(255)`,
		`ALTER TABLE "user-persona" ADD COLUMN IF NOT EXISTS account_bank VARCHAR(255)`,
		`ALTER TABLE "user-persona" ADD COLUMN IF NOT EXISTS account_number VARCHAR(255)`,
		`ALTER TABLE "user-persona" ADD COLUMN IF NOT EXISTS account_name VARCHAR(255)`,
		`ALTER TABLE "user-persona" ADD COLUMN IF NOT EXISTS city_id UUID`,
		`ALTER TABLE "user-persona" ADD COLUMN IF NOT EXISTS approved_by UUID`,
		`ALTER TABLE "user-persona" ADD COLUMN IF NOT EXISTS approved_at TIMESTAMP WITH TIME ZONE`,
	}

	for _, query := range alterTableQueries {
		_, err := devIdentityDB.Exec(query)
		if err != nil {
			log.Fatal("Error saat menambahkan kolom:", err)
		}
	}

	var totalRows int
	err := devIdentityDB.QueryRow(`SELECT COUNT(*) FROM "user"`).Scan(&totalRows)
	if err != nil {
		log.Fatal("Error saat menghitung total rows:", err)
	}

	fmt.Printf("Total wukala yang akan diproses: %d\n", totalRows)

	bar := progressbar.NewOptions(totalRows,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription("[cyan][1/1][reset] Memproses data persona wukala..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
	)

	checkStmt, err := devIdentityDB.Prepare(`
		SELECT COUNT(*) FROM "user-persona" WHERE id = $1
	`)
	if err != nil {
		log.Fatal("Error preparing check statement:", err)
	}
	defer checkStmt.Close()

	// Modified to check for existing phone numbers
	checkPhoneStmt, err := devIdentityDB.Prepare(`
		SELECT COUNT(*) FROM "user-persona" WHERE phone_number = $1 AND id != $2
	`)
	if err != nil {
		log.Fatal("Error preparing check phone statement:", err)
	}
	defer checkPhoneStmt.Close()

	insertStmt, err := devIdentityDB.Prepare(`
		INSERT INTO "user-persona" (
			id, phone_number, travel_id, "desc", code, fee,
			web_visit, activated_at, discount, parent_id,
			fee_type, discount_type, bdm_user_id, alias,
			nik, instagram, account_bank, account_number,
			account_name, address, city_id, approved_by,
			approved_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
			$11, $12, $13, $14, $15, $16, $17, $18, $19,
			$20, $21, $22, $23
		)
	`)
	if err != nil {
		log.Fatal("Error preparing insert statement:", err)
	}
	defer insertStmt.Close()

	updateStmt, err := devIdentityDB.Prepare(`
		UPDATE "user-persona" SET
			phone_number = $2,
			travel_id = CASE WHEN $3::UUID IS NULL THEN travel_id ELSE $3::UUID END,
			"desc" = $4,
			code = $5,
			fee = $6,
			web_visit = $7,
			activated_at = $8,
			discount = $9,
			parent_id = CASE WHEN $10::UUID IS NULL THEN parent_id ELSE $10::UUID END,
			fee_type = $11,
			discount_type = $12,
			bdm_user_id = CASE WHEN $13::UUID IS NULL THEN bdm_user_id ELSE $13::UUID END,
			alias = $14,
			nik = $15,
			instagram = $16,
			account_bank = $17,
			account_number = $18,
			account_name = $19,
			address = $20,
			city_id = CASE WHEN $21::UUID IS NULL THEN city_id ELSE $21::UUID END,
			approved_by = CASE WHEN $22::UUID IS NULL THEN approved_by ELSE $22::UUID END,
			approved_at = $23
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
		duplicateCount  int
		startTime       = time.Now()
		processedRows   = 0
		duplicatePhones = make([]DuplicatePhoneInfoWukala, 0)
	)

	rows, err := devIdentityDB.Query(`SELECT id FROM "user"`)
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
			travelID      sql.NullString
			phone         sql.NullString
			desc          sql.NullString
			code          sql.NullString
			fee           sql.NullFloat64
			webVisit      sql.NullInt64
			activatedAt   sql.NullTime
			discount      sql.NullFloat64
			parentID      sql.NullString
			feeType       sql.NullString
			discountType  sql.NullString
			rdaID         sql.NullString
			alias         sql.NullString
			nik           sql.NullString
			instagram     sql.NullString
			accountBank   sql.NullString
			accountNumber sql.NullString
			accountName   sql.NullString
			address       sql.NullString
			cityID        sql.NullString
			approvedBy    sql.NullString
			approvedAt    sql.NullTime
		)

		err = prodExistingUmrahDB.QueryRow(`
			SELECT 
				travel_id, phone, "desc", code, fee, web_visit,
				activated_at, discount, parent_id, fee_type,
				discount_type, rda_id, alias, nik, instagram,
				account_bank, account_number, account_name,
				address, city_id, approved_by, approved_at
			FROM td_travel_agent WHERE user_id = $1
		`, userID).Scan(
			&travelID, &phone, &desc, &code, &fee, &webVisit,
			&activatedAt, &discount, &parentID, &feeType,
			&discountType, &rdaID, &alias, &nik, &instagram,
			&accountBank, &accountNumber, &accountName,
			&address, &cityID, &approvedBy, &approvedAt,
		)

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

		// Check for duplicate phone number
		if phone.Valid && phone.String != "" {
			var count int
			err = checkPhoneStmt.QueryRow(phone.String, userID).Scan(&count)
			if err != nil {
				log.Printf("Error checking duplicate phone: %v", err)
			} else if count > 0 {
				// Record duplicate phone number
				duplicatePhones = append(duplicatePhones, DuplicatePhoneInfoWukala{
					UserID:      userID,
					PhoneNumber: phone.String,
				})
				// Set phone to NULL
				phone.Valid = false
				phone.String = ""
				duplicateCount++
			}
		}

		// Truncate phone number if necessary
		if phone.Valid && len(phone.String) > 16 {
			phone.String = phone.String[:16]
		}

		var exists int
		err = checkStmt.QueryRow(userID).Scan(&exists)
		if err != nil {
			log.Printf("Error checking existing record: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		// Handle NULL values for UUID fields
		var (
			travelIDValue   interface{} = nil
			parentIDValue   interface{} = nil
			rdaIDValue      interface{} = nil
			cityIDValue     interface{} = nil
			approvedByValue interface{} = nil
		)

		if travelID.Valid {
			travelIDValue = travelID.String
		}
		if parentID.Valid {
			parentIDValue = parentID.String
		}
		if rdaID.Valid {
			rdaIDValue = rdaID.String
		}
		if cityID.Valid {
			cityIDValue = cityID.String
		}
		if approvedBy.Valid {
			approvedByValue = approvedBy.String
		}

		if exists > 0 {
			_, err = updateStmt.Exec(
				userID, sql.NullString{String: phone.String, Valid: phone.Valid}, travelIDValue, desc.String,
				code.String, fee.Float64, webVisit.Int64, activatedAt.Time,
				discount.Float64, parentIDValue, feeType.String,
				discountType.String, rdaIDValue, alias.String,
				nik.String, instagram.String, accountBank.String,
				accountNumber.String, accountName.String, address.String,
				cityIDValue, approvedByValue, approvedAt.Time,
			)
			if err != nil {
				log.Printf("Error updating record for user %s: %v", userID, err)
				errorCount++
			} else {
				updateCount++
			}
		} else {
			_, err = insertStmt.Exec(
				userID, sql.NullString{String: phone.String, Valid: phone.Valid}, travelIDValue, desc.String,
				code.String, fee.Float64, webVisit.Int64, activatedAt.Time,
				discount.Float64, parentIDValue, feeType.String,
				discountType.String, rdaIDValue, alias.String,
				nik.String, instagram.String, accountBank.String,
				accountNumber.String, accountName.String, address.String,
				cityIDValue, approvedByValue, approvedAt.Time,
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
	fmt.Printf("\nTransfer data persona wukala selesai!\n")
	fmt.Printf("\nRingkasan Hasil:\n")
	fmt.Printf("---------------\n")
	fmt.Printf("Total records diproses: %d\n", processedRows)
	fmt.Printf("Records berhasil ditambahkan: %d\n", insertCount)
	fmt.Printf("Records berhasil diperbarui: %d\n", updateCount)
	fmt.Printf("Records dilewati: %d\n", skippedCount)
	fmt.Printf("Records gagal: %d\n", errorCount)
	fmt.Printf("Nomor telepon duplikat: %d\n", duplicateCount)
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
