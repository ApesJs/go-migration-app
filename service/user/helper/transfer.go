package helper

import (
	"database/sql"
	"fmt"
	"github.com/schollz/progressbar/v3"
	"log"
	"time"
)

func TransferData(sourceDB *sql.DB, txStmts *TxStatements, bar *progressbar.ProgressBar) *TransferStats {
	stats := &TransferStats{
		SkippedTravelAgent: make(map[string]string),
	}

	// Query untuk mengambil data user
	rows, err := sourceDB.Query(`
		SELECT id, name, email, role, image, soft_delete, created_at, updated_at 
		FROM td_user tu 
		WHERE role = 'user' AND soft_delete = 'false'
	`)
	if err != nil {
		log.Fatal("Error querying source database:", err)
	}
	defer rows.Close()

	// Statement untuk cek travel agent
	checkTravelAgentStmt, err := sourceDB.Prepare(`
		SELECT EXISTS(
			SELECT 1 
			FROM td_travel_agent t
			WHERE t.user_id = $1 
		)
	`)
	if err != nil {
		log.Fatal("Error preparing travel agent check statement:", err)
	}
	defer checkTravelAgentStmt.Close()

	for rows.Next() {
		processRow(rows, txStmts, checkTravelAgentStmt, stats, bar)
	}

	return stats
}

func processRow(
	rows *sql.Rows,
	txStmts *TxStatements,
	checkTravelAgentStmt *sql.Stmt,
	stats *TransferStats,
	bar *progressbar.ProgressBar,
) {
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

	// Scan data dari source database
	if err := rows.Scan(
		&id,
		&name,
		&email,
		&role,
		&image,
		&softDelete,
		&createdAt,
		&updatedAt,
	); err != nil {
		log.Printf("Error scanning row: %v", err)
		stats.ErrorCount++
		bar.Add(1)
		return
	}

	// Cek apakah user adalah travel agent
	var isTravelAgent bool
	err := checkTravelAgentStmt.QueryRow(id).Scan(&isTravelAgent)
	if err != nil {
		log.Printf("Error checking travel agent status: %v", err)
		stats.ErrorCount++
		bar.Add(1)
		return
	}

	// Cek apakah email sudah ada di database target
	var count int
	err = txStmts.Check.QueryRow(email).Scan(&count)
	if err != nil {
		log.Printf("Error checking for duplicate email: %v", err)
		stats.ErrorCount++
		bar.Add(1)
		return
	}

	// Jika email sudah ada dan user adalah travel agent, catat dalam skippedTravelAgent
	if count > 0 {
		stats.DuplicateEmails = append(
			stats.DuplicateEmails,
			fmt.Sprintf("%s (%s)", email, name),
		)
		if isTravelAgent {
			stats.SkippedTravelAgent[email] = name
		}
		stats.SkipCount++
		bar.Add(1)
		return
	}

	// Tentukan role berdasarkan status travel agent
	finalRole := role
	if isTravelAgent {
		finalRole = "wukala"
		stats.WukalaCount++
	}

	// Insert ke target database
	_, err = txStmts.Insert.Exec(
		id,          // id
		name,        // name
		email,       // username (dari email)
		email,       // email
		finalRole,   // role (wukala jika travel agent, original role jika bukan)
		image,       // avatar_provider (dari image)
		softDelete,  // deleted (dari soft_delete)
		createdAt,   // created_at
		updatedAt,   // modified_at (dari updated_at)
		"migration", // created_by
	)
	if err != nil {
		log.Printf("Error inserting row: %v", err)
		stats.ErrorCount++
		bar.Add(1)
		return
	}

	stats.TransferredCount++
	bar.Add(1)
}
