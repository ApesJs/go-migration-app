package helper

import "database/sql"

func GetCityIDFromLocationStmt(devIdentityDB *sql.DB) (*sql.Stmt, error) {
	return devIdentityDB.Prepare(`
        SELECT id 
        FROM location_city 
        WHERE name = $1
        LIMIT 1
    `)
}

func InsertAirportStmt(devGeneralDB *sql.DB) (*sql.Stmt, error) {
	return devGeneralDB.Prepare(`
        INSERT INTO airport (
            name, code, country_name, country_id,
            city_name, city_id, created_by, modified_by
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8
        ) RETURNING id
    `)
}

func CheckAirportExistStmt(devGeneralDB *sql.DB) (*sql.Stmt, error) {
	return devGeneralDB.Prepare(`
        SELECT id 
        FROM airport 
        WHERE code = $1
        LIMIT 1
    `)
}

func TotalAirports(data []AirportJSON) int {
	return len(data)
}
