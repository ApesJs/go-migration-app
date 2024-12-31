package helper

import (
	"database/sql"
	"fmt"
)

func GetAllPackageHotelsStmt(localUmrahDB *sql.DB) (*sql.Stmt, error) {
	return localUmrahDB.Prepare(`
        SELECT DISTINCT medina_hotel, mecca_hotel
        FROM package
        WHERE medina_hotel IS NOT NULL 
        OR mecca_hotel IS NOT NULL
    `)
}

func GetCityIDStmt(devIdentityDB *sql.DB) (*sql.Stmt, error) {
	return devIdentityDB.Prepare(`
        SELECT id 
        FROM location_city 
        WHERE name = $1
        LIMIT 1
    `)
}

func CheckHotelExistStmt(devGeneralDB *sql.DB) (*sql.Stmt, error) {
	return devGeneralDB.Prepare(`
        SELECT id 
        FROM hotel 
        WHERE name = $1
        LIMIT 1
    `)
}

func InsertHotelStmt(devGeneralDB *sql.DB) (*sql.Stmt, error) {
	return devGeneralDB.Prepare(`
        INSERT INTO hotel (
            name, address, city_name, city_id, rating,
            logo, created_at, modified_at, created_by, modified_by
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
        ) RETURNING id
    `)
}

func UpdatePackageHotelIDStmt(localUmrahDB *sql.DB, field string) (*sql.Stmt, error) {
	return localUmrahDB.Prepare(fmt.Sprintf(`
        UPDATE package 
        SET %s = jsonb_set(
            %s,
            '{id}',
            $1::text::jsonb
        )
        WHERE %s->>'name' = $2
    `, field, field, field))
}

func TotalHotels(devUmrahDB *sql.DB) (int, error) {
	var totalHotels int
	err := devUmrahDB.QueryRow(`
        SELECT COUNT(*) 
        FROM (
            SELECT 1
            FROM package p
            WHERE (p.medina_hotel IS NOT NULL AND p.medina_hotel != 'null'::jsonb)
            OR (p.mecca_hotel IS NOT NULL AND p.mecca_hotel != 'null'::jsonb)
        ) h
    `).Scan(&totalHotels)

	return totalHotels, err
}
