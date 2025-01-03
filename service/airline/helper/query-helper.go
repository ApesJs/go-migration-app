package helper

import "database/sql"

// InsertAirlineStmt prepares insert statement for airline
func InsertAirlineStmt(db *sql.DB) (*sql.Stmt, error) {
	return db.Prepare(`
        INSERT INTO airline (
            name, code, country_name, country_id,
            logo, created_by, modified_by
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7
        )
    `)
}

// CheckExistingAirlineStmt prepares statement to check if airline code exists
func CheckExistingAirlineStmt(db *sql.DB) (*sql.Stmt, error) {
	return db.Prepare(`
        SELECT COUNT(*) 
        FROM airline 
        WHERE code = $1
    `)
}
