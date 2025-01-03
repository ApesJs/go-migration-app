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

// GetNewAirlineIDStmt gets new airline ID by code
func GetNewAirlineIDStmt(db *sql.DB) (*sql.Stmt, error) {
	return db.Prepare(`
        SELECT id 
        FROM airline 
        WHERE code = $1
    `)
}

// query-helper.go
func UpdatePackageDepartureAirlineStmt(db *sql.DB) (*sql.Stmt, error) {
	return db.Prepare(`
        UPDATE package 
        SET departure = jsonb_set(
            jsonb_set(
                departure,
                '{airline,id}',
                $1::text::jsonb,
                false
            ),
            '{airlineId}',
            $1::text::jsonb,
            false
        )
        WHERE departure->'airline'->>'name' = $2
    `)
}

func UpdatePackageArrivalAirlineStmt(db *sql.DB) (*sql.Stmt, error) {
	return db.Prepare(`
        UPDATE package 
        SET arrival = jsonb_set(
            jsonb_set(
                arrival,
                '{airline,id}',
                $1::text::jsonb,
                false
            ),
            '{airlineId}',
            $1::text::jsonb,
            false
        )
        WHERE arrival->'airline'->>'name' = $2
    `)
}
