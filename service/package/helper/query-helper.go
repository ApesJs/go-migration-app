package helper

import (
	"database/sql"
)

func TotalRows(prodExistingUmrahDB *sql.DB) (int, error) {
	var totalRows int
	err := prodExistingUmrahDB.QueryRow(`
		SELECT COUNT(*) 
		FROM td_package 
		WHERE soft_delete = false 
		AND departure_date < CURRENT_TIMESTAMP`).Scan(&totalRows)

	return totalRows, err
}
