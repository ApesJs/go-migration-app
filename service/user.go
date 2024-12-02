package service

import (
	"database/sql"
	"fmt"
	"github.com/ApesJs/go-migration-app/database"
	"github.com/schollz/progressbar/v3"
	"log"
	"time"
)

func UserService() {
	//Panggil Koneksi Database
	sourceDB, targetDB := database.ConnectionDB()
	defer sourceDB.Close()
	defer targetDB.Close()

	// Pastikan role 'wukala' ada di tabel role
	var roleExists bool
	err := targetDB.QueryRow(`SELECT EXISTS(SELECT 1 FROM "role" WHERE slug = $1)`, "wukala").Scan(&roleExists)
	if err != nil {
		log.Fatal("Error checking wukala role existence:", err)
	}

	if !roleExists {
		_, err = targetDB.Exec(`INSERT INTO "role" (name, slug) VALUES ($1, $2)`, "Wukala", "wukala")
		if err != nil {
			log.Fatal("Error inserting wukala role:", err)
		}
		fmt.Println("Created 'wukala' role")
	}

	// Menghitung total records yang akan ditransfer
	var totalRows int
	err = sourceDB.QueryRow("SELECT COUNT(*) FROM td_user WHERE role = 'user'").Scan(&totalRows)
	if err != nil {
		log.Fatal("Error counting rows:", err)
	}

	// Menghitung total travel agents
	var totalTravelAgents int
	err = sourceDB.QueryRow(`
		SELECT COUNT(*) 
		FROM td_user u
		JOIN td_travel_agent t ON u.id = t.user_id
		WHERE u.role = 'user'
	`).Scan(&totalTravelAgents)
	if err != nil {
		log.Fatal("Error counting travel agents:", err)
	}

	fmt.Printf("Found %d total records to transfer\n", totalRows)
	fmt.Printf("Found %d travel agents in source database\n", totalTravelAgents)

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

	// Prepare statement untuk cek travel agent
	checkTravelAgentStmt, err := sourceDB.Prepare(`
		SELECT EXISTS(SELECT 1 FROM td_travel_agent WHERE user_id = $1)
	`)
	if err != nil {
		log.Fatal("Error preparing travel agent check statement:", err)
	}
	defer checkTravelAgentStmt.Close()

	// Mengambil data dari database sumber
	rows, err := sourceDB.Query("SELECT id, name, email, role, google_id, image, soft_delete, created_at, updated_at FROM td_user WHERE role = 'user'")
	if err != nil {
		log.Fatal("Error querying source database:", err)
	}
	defer rows.Close()

	// Prepare statement untuk mengecek duplikasi
	checkStmt, err := targetDB.Prepare(`SELECT COUNT(*) FROM "user" WHERE email = $1`)
	if err != nil {
		log.Fatal("Error preparing check statement:", err)
	}
	defer checkStmt.Close()

	// Prepare statement untuk insert
	insertStmt, err := targetDB.Prepare(`
		INSERT INTO "user" (
			id, name, username, email, role,
			google_id, is_active, email_verified,
			avatar, avatar_provider, provider,
			deleted, created_at, modified_at,
			created_by, modified_by
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, true, false,
			null, $7, null,
			$8, $9, $10,
			null, null
		)
	`)
	if err != nil {
		log.Fatal("Error preparing insert statement:", err)
	}
	defer insertStmt.Close()

	// Variabel untuk statistik
	var (
		transferredCount   int
		errorCount         int
		skipCount          int
		wukalaCount        int
		duplicateEmails    []string                  // Slice untuk menyimpan email yang duplikat
		skippedTravelAgent = make(map[string]string) // Map untuk menyimpan travel agent yang di-skip
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
		google_id  sql.NullString
		image      sql.NullString
		softDelete sql.NullBool
		createdAt  time.Time
		updatedAt  time.Time
	)

	for rows.Next() {
		// Scan data dari source database
		err := rows.Scan(&id, &name, &email, &role, &google_id, &image, &softDelete, &createdAt, &updatedAt)
		if err != nil {
			log.Printf("Error scanning row: %v", err)
			errorCount++
			bar.Add(1)
			continue
		}

		// Cek apakah user adalah travel agent
		var isTravelAgent bool
		err = checkTravelAgentStmt.QueryRow(id).Scan(&isTravelAgent)
		if err != nil {
			log.Printf("Error checking travel agent status: %v", err)
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

		// Jika email sudah ada dan user adalah travel agent, catat dalam skippedTravelAgent
		if count > 0 {
			duplicateEmails = append(duplicateEmails, fmt.Sprintf("%s (%s)", email, name))
			if isTravelAgent {
				skippedTravelAgent[email] = name
			}
			skipCount++
			bar.Add(1)
			continue
		}

		// Tentukan role berdasarkan status travel agent
		finalRole := role
		if isTravelAgent {
			finalRole = "wukala"
			wukalaCount++
		}

		// Insert ke target database
		_, err = txInsertStmt.Exec(
			id,         // id
			name,       // name
			email,      // username (dari email)
			email,      // email
			finalRole,  // role (wukala jika travel agent, original role jika bukan)
			google_id,  // google_id
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
	fmt.Printf("Total travel agents in source: %d\n", totalTravelAgents)
	fmt.Printf("Successfully transferred: %d\n", transferredCount)
	fmt.Printf("Converted to wukala: %d\n", wukalaCount)
	fmt.Printf("Skipped (duplicates): %d\n", skipCount)
	fmt.Printf("Failed transfers: %d\n", errorCount)
	fmt.Printf("Duration: %s\n", duration.Round(time.Second))
	fmt.Printf("Average speed: %.2f records/second\n", float64(transferredCount)/duration.Seconds())

	// Menampilkan list email duplikat
	if len(duplicateEmails) > 0 {
		fmt.Printf("\nDuplicate Emails:\n")
		fmt.Printf("----------------\n")
		for i, email := range duplicateEmails {
			fmt.Printf("%d. %s\n", i+1, email)
		}
	}

	// Menampilkan travel agents yang di-skip
	if len(skippedTravelAgent) > 0 {
		fmt.Printf("\nSkipped Travel Agents (already exists):\n")
		fmt.Printf("------------------------------------\n")
		i := 1
		for email, name := range skippedTravelAgent {
			fmt.Printf("%d. %s (%s)\n", i, email, name)
			i++
		}
		fmt.Printf("\nTotal travel agents skipped: %d\n", len(skippedTravelAgent))
		fmt.Printf("Expected travel agents: %d\n", totalTravelAgents)
		fmt.Printf("Actual converted: %d\n", wukalaCount)
		fmt.Printf("Difference: %d\n", totalTravelAgents-wukalaCount)
	}
}
