package helper

import (
	"database/sql"
	"log"
)

func TotalRows(prodExistingUmrahDB *sql.DB) int {
	var totalRows int
	err := prodExistingUmrahDB.QueryRow(`
		SELECT COUNT(*) 
		FROM td_package 
		WHERE soft_delete = false 
		AND departure_date < CURRENT_TIMESTAMP`).Scan(&totalRows)
	if err != nil {
		log.Fatal("Error counting rows:", err)
	}

	return totalRows
}
