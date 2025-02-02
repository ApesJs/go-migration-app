package user

import (
	"database/sql"
	"fmt"
	"github.com/ApesJs/go-migration-app/database"
	"github.com/schollz/progressbar/v3"
	"log"
	"math"
	"strings"
	"time"
)

type DuplicatePhoneInfoWukala struct {
	UserID      string
	PhoneNumber string
}

func WukalaPersonaService() {
	prodExistingUmrahDB := database.ConnectionProdExistingUmrahDB()
	defer prodExistingUmrahDB.Close()

	//devUmrahDB := database.ConnectionProdUmrahDB()
	//defer devUmrahDB.Close()

	//devIdentityDB := database.ConnectionProdIdentityDB()
	//defer devIdentityDB.Close()

	devUmrahDB := database.ConnectionDevUmrahDB()
	defer devUmrahDB.Close()

	devIdentityDB := database.ConnectionDevIdentityDB()
	defer devIdentityDB.Close()

	//localIdentityDB := database.ConnectionLocalIdentityDB()
	//defer localIdentityDB.Close()

	//localUmrahDB := database.ConnectionLocalUmrahDB()
	//defer localUmrahDB.Close()

	fmt.Println("Memulai proses transfer data persona wukala...")

	// Start transactions for both target databases
	identityTx, err := devIdentityDB.Begin()
	if err != nil {
		log.Fatalf("error starting identity transaction: %v", err)
	}
	defer identityTx.Rollback()

	umrahTx, err := devUmrahDB.Begin()
	if err != nil {
		log.Fatalf("error starting umrah transaction: %v", err)
	}
	defer umrahTx.Rollback()

	// Add new columns to user_persona table
	alterTableQueries := []string{
		`ALTER TABLE "user_persona" ADD COLUMN IF NOT EXISTS travel_id UUID`,
		`ALTER TABLE "user_persona" ADD COLUMN IF NOT EXISTS "desc" TEXT`,
		`ALTER TABLE "user_persona" ADD COLUMN IF NOT EXISTS web_visit INTEGER`,
		`ALTER TABLE "user_persona" ADD COLUMN IF NOT EXISTS activated_at TIMESTAMP WITH TIME ZONE`,
		`ALTER TABLE "user_persona" ADD COLUMN IF NOT EXISTS parent_id UUID`,
		`ALTER TABLE "user_persona" ADD COLUMN IF NOT EXISTS bdm_user_id UUID`,
		`ALTER TABLE "user_persona" ADD COLUMN IF NOT EXISTS alias VARCHAR(50)`,
		`ALTER TABLE "user_persona" ADD COLUMN IF NOT EXISTS nik VARCHAR(20)`,
		`ALTER TABLE "user_persona" ADD COLUMN IF NOT EXISTS instagram VARCHAR(255)`,
		`ALTER TABLE "user_persona" ADD COLUMN IF NOT EXISTS account_bank VARCHAR(255)`,
		`ALTER TABLE "user_persona" ADD COLUMN IF NOT EXISTS account_number VARCHAR(255)`,
		`ALTER TABLE "user_persona" ADD COLUMN IF NOT EXISTS account_name VARCHAR(255)`,
		`ALTER TABLE "user_persona" ADD COLUMN IF NOT EXISTS city_id UUID`,
		`ALTER TABLE "user_persona" ADD COLUMN IF NOT EXISTS approved_by UUID`,
		`ALTER TABLE "user_persona" ADD COLUMN IF NOT EXISTS approved_at TIMESTAMP WITH TIME ZONE`,
	}

	for _, query := range alterTableQueries {
		_, err := identityTx.Exec(query)
		if err != nil {
			log.Fatalf("error saat menambahkan kolom: %v", err)
		}
	}

	var totalRows int
	err = devIdentityDB.QueryRow(`SELECT COUNT(*) FROM "user" WHERE role = 'wukala'`).Scan(&totalRows)
	if err != nil {
		log.Fatalf("error saat menghitung total rows: %v", err)
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

	// Prepare statements for user_persona
	checkPersonaStmt, err := identityTx.Prepare(`
		SELECT COUNT(*) FROM "user_persona" WHERE id = $1
	`)
	if err != nil {
		log.Fatalf("error preparing check persona statement: %v", err)
	}
	defer checkPersonaStmt.Close()

	// Tambahkan fungsi untuk mengecek duplikasi code
	checkCodeStmt, err := identityTx.Prepare(`
    SELECT COUNT(*) FROM "user_persona" WHERE code = $1 AND id != $2
`)
	if err != nil {
		log.Fatalf("error preparing check code statement: %v", err)
	}
	defer checkCodeStmt.Close()

	checkPhoneStmt, err := identityTx.Prepare(`
		SELECT COUNT(*) FROM "user_persona" WHERE phone_number = $1 AND id != $2
	`)
	if err != nil {
		log.Fatalf("error preparing check phone statement: %v", err)
	}
	defer checkPhoneStmt.Close()

	insertPersonaStmt, err := identityTx.Prepare(`
		INSERT INTO "user_persona" (
			id, phone_number, travel_id, "desc",
			web_visit, activated_at, parent_id,
			bdm_user_id, alias, nik, instagram,
			account_bank, account_number, account_name,
			address, city_id, approved_by, approved_at,
			code, fee_type, fee, discount_type, discount
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9,
			$10, $11, $12, $13, $14, $15, $16, $17, $18,
			NULLIF($19, ''), $20, $21, $22, $23
		)
	`)
	if err != nil {
		log.Fatalf("error preparing insert persona statement: %v", err)
	}
	defer insertPersonaStmt.Close()

	updatePersonaStmt, err := identityTx.Prepare(`
		UPDATE "user_persona" SET
			phone_number = $2,
			travel_id = CASE WHEN $3::UUID IS NULL THEN travel_id ELSE $3::UUID END,
			"desc" = $4,
			web_visit = $5,
			activated_at = $6,
			parent_id = $7,
			bdm_user_id = $8,
			alias = $9,
			nik = $10,
			instagram = $11,
			account_bank = $12,
			account_number = $13,
			account_name = $14,
			address = $15,
			city_id = $16,
			approved_by = CASE WHEN $17::UUID IS NULL THEN approved_by ELSE $17::UUID END,
			approved_at = $18,
			code = NULLIF($19, ''),
			fee_type = $20,
			fee = $21,
			discount_type = $22,
			discount = $23
		WHERE id = $1
	`)
	if err != nil {
		log.Fatalf("error preparing update persona statement: %v", err)
	}
	defer updatePersonaStmt.Close()

	// Prepare statements for wukala_setting
	checkSettingStmt, err := umrahTx.Prepare(`
		SELECT COUNT(*) FROM "wukala_setting" WHERE referral_code = $1
	`)
	if err != nil {
		log.Fatalf("error preparing check setting statement: %v", err)
	}
	defer checkSettingStmt.Close()

	insertSettingStmt, err := umrahTx.Prepare(`
		INSERT INTO "wukala_setting" (
			referral_code, fee_type, fee_amount,
			discount_type, discount_amount, created_at,
			modified_at, created_by, modified_by
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9
		)
	`)
	if err != nil {
		log.Fatalf("error preparing insert setting statement: %v", err)
	}
	defer insertSettingStmt.Close()

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

	rows, err := devIdentityDB.Query(`SELECT id FROM "user" WHERE role = 'wukala'`)
	if err != nil {
		log.Fatalf("error querying user data: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var userID string
		err := rows.Scan(&userID)
		if err != nil {
			log.Fatalf("error scanning user ID: %v", err)
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
			createdAt     sql.NullTime
			updatedAt     sql.NullTime
		)

		err = prodExistingUmrahDB.QueryRow(`
			SELECT 
				travel_id, phone, "desc", code, fee, web_visit,
				activated_at, discount, parent_id, fee_type,
				discount_type, rda_id, alias, nik, instagram,
				account_bank, account_number, account_name,
				address, city_id, approved_by, approved_at,
				created_at, updated_at
			FROM td_travel_agent WHERE user_id = $1
		`, userID).Scan(
			&travelID, &phone, &desc, &code, &fee, &webVisit,
			&activatedAt, &discount, &parentID, &feeType,
			&discountType, &rdaID, &alias, &nik, &instagram,
			&accountBank, &accountNumber, &accountName,
			&address, &cityID, &approvedBy, &approvedAt,
			&createdAt, &updatedAt,
		)

		if err == sql.ErrNoRows {
			skippedCount++
			bar.Add(1)
			continue
		} else if err != nil {
			log.Fatalf("error querying source data for user %s: %v", userID, err)
		}

		// Check for duplicate phone number
		if phone.Valid && phone.String != "" {
			var count int
			err = checkPhoneStmt.QueryRow(phone.String, userID).Scan(&count)
			if err != nil {
				log.Fatalf("error checking duplicate phone: %v", err)
			}
			if count > 0 {
				duplicatePhones = append(duplicatePhones, DuplicatePhoneInfoWukala{
					UserID:      userID,
					PhoneNumber: phone.String,
				})
				phone.Valid = false
				phone.String = ""
				duplicateCount++
			}
		}

		// Truncate phone number if necessary
		if phone.Valid && len(phone.String) > 16 {
			phone.String = phone.String[:16]
		}

		// Process wukala_setting first
		if code.Valid && code.String != "" {
			var codeCount int
			err = checkCodeStmt.QueryRow(code.String, userID).Scan(&codeCount)
			if err != nil {
				log.Fatalf("error checking duplicate code: %v", err)
			}
			if codeCount > 0 {
				// Jika code duplikat, set menjadi empty string
				code.String = ""
				code.Valid = false
				log.Printf("Warning: Duplicate code found for user %s, setting to empty", userID)
			}

			// Truncate code to 8 chars if needed
			referralCode := code.String
			if len(referralCode) > 8 {
				referralCode = referralCode[:8]
			}

			// Convert fee and discount to integer
			feeAmount := 0
			if fee.Valid {
				feeAmount = int(math.Round(fee.Float64))
			}

			discountAmount := 0
			if discount.Valid {
				discountAmount = int(math.Round(discount.Float64))
			}

			// Truncate types to 12 chars if needed
			feeTypeStr := "nominal"
			if feeType.Valid {
				feeTypeStr = feeType.String
				if len(feeTypeStr) > 12 {
					feeTypeStr = feeTypeStr[:12]
				}
			}

			discountTypeStr := "nominal"
			if discountType.Valid {
				discountTypeStr = discountType.String
				if len(discountTypeStr) > 12 {
					discountTypeStr = discountTypeStr[:12]
				}
			}

			// Check if referral_code already exists
			var codeExists int
			err = checkSettingStmt.QueryRow(referralCode).Scan(&codeExists)
			if err != nil {
				log.Fatalf("error checking existing referral code: %v", err)
			}

			if codeExists == 0 {
				// Set default time if NULL
				var (
					effectiveCreatedAt  time.Time
					effectiveModifiedAt time.Time
				)

				if createdAt.Valid {
					effectiveCreatedAt = createdAt.Time
				} else {
					effectiveCreatedAt = time.Now()
				}

				if updatedAt.Valid {
					effectiveModifiedAt = updatedAt.Time
				} else {
					effectiveModifiedAt = time.Now()
				}

				_, err = insertSettingStmt.Exec(
					referralCode,
					strings.ToLower(feeTypeStr),
					feeAmount,
					strings.ToLower(discountTypeStr),
					discountAmount,
					effectiveCreatedAt,
					effectiveModifiedAt,
					"migration",
					nil,
				)
				if err != nil {
					log.Fatalf("error inserting wukala setting for user %s: %v", userID, err)
				}
			}
		}

		// Process user_persona
		var exists int
		err = checkPersonaStmt.QueryRow(userID).Scan(&exists)
		if err != nil {
			log.Fatalf("error checking existing record: %v", err)
		}

		// Handle UUID values
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

		// Handle alias value
		var aliasValue sql.NullString
		if alias.Valid && alias.String != "" {
			aliasValue = alias
		} else {
			aliasValue = sql.NullString{Valid: false}
		}

		if exists > 0 {
			_, err = updatePersonaStmt.Exec(
				userID, sql.NullString{String: phone.String, Valid: phone.Valid},
				travelIDValue, desc.String, webVisit.Int64, activatedAt.Time,
				parentIDValue, rdaIDValue, aliasValue,
				nik.String, instagram.String, accountBank.String,
				accountNumber.String, accountName.String, address.String,
				cityIDValue, approvedByValue, approvedAt.Time,
				code.String,  // code
				feeType,      // fee_type
				fee,          // fee
				discountType, // discount_type
				discount,     // discount
			)
			if err != nil {
				log.Fatalf("error updating record for user %s: %v", userID, err)
			}
			updateCount++
		} else {
			_, err = insertPersonaStmt.Exec(
				userID, sql.NullString{String: phone.String, Valid: phone.Valid},
				travelIDValue, desc.String, webVisit.Int64, activatedAt.Time,
				parentIDValue, rdaIDValue, aliasValue,
				nik.String, instagram.String, accountBank.String,
				accountNumber.String, accountName.String, address.String,
				cityIDValue, approvedByValue, approvedAt.Time,
				code.String,  // code
				feeType,      // fee_type
				fee,          // fee
				discountType, // discount_type
				discount,     // discount
			)
			if err != nil {
				log.Fatalf("error inserting record for user %s: %v", userID, err)
			}
			insertCount++
		}

		processedRows++
		bar.Add(1)
	}

	// If we've made it here, commit both transactions
	err = identityTx.Commit()
	if err != nil {
		log.Fatalf("error committing identity transaction: %v", err)
	}

	err = umrahTx.Commit()
	if err != nil {
		log.Fatalf("error committing umrah transaction: %v", err)
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
