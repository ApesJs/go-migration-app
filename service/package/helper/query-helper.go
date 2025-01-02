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
		AND departure_date >= '2025-01-10'`).Scan(&totalRows)

	return totalRows, err
}

func OrgInsStmt(devIdentityDB *sql.DB) (*sql.Stmt, error) {
	return devIdentityDB.Prepare(`
        SELECT id, name 
        FROM organization_instance 
        WHERE organization_id = $1
        LIMIT 1
    `)
}

func TravelStmt(prodExistingUmrahDB *sql.DB) (*sql.Stmt, error) {
	return prodExistingUmrahDB.Prepare(`
        SELECT name 
        FROM td_travel 
        WHERE id = $1
    `)
}

func HotelStmt(prodExistingUmrahDB *sql.DB) (*sql.Stmt, error) {
	return prodExistingUmrahDB.Prepare(`
        SELECT h.id, h.name, h.address, h.rate, h.logo, h.created_at, h.updated_at,
               c.id as city_id, c.name as city_name
        FROM td_package_hotel ph
        JOIN td_hotel h ON ph.hotel_id = h.id
        JOIN td_city c ON h.city_id = c.id
        WHERE ph.package_id = $1
    `)
}

func AirlineStmt(prodExistingUmrahDB *sql.DB) (*sql.Stmt, error) {
	return prodExistingUmrahDB.Prepare(`
        SELECT code, logo, name, created_at, updated_at
        FROM td_airline 
        WHERE id = $1
    `)
}

func InsertPackageStmt(localUmrahDB *sql.DB) (*sql.Stmt, error) {
	return localUmrahDB.Prepare(`
        INSERT INTO package (
            organization_id, organization_instance_id, package_type,
            thumbnail, title, description, terms_condition,
            facility, currency, medina_hotel, mecca_hotel,
            departure, arrival, dp_type, dp_amount,
            fee_type, fee_amount, deleted, created_at,
            modified_at, created_by, modified_by,
            organization_instance_name, organization_instance,
            slug, published
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16, $17, $18,
            $19, $20, $21, $22, $23, $24, $25, $26
        ) RETURNING id
    `)
}

func InsertVariantStmt(localUmrahDB *sql.DB) (*sql.Stmt, error) {
	return localUmrahDB.Prepare(`
        INSERT INTO package_variant (
            package_id, thumbnail, name, departure_date, arrival_date,
            original_price_double, original_price_triple, original_price_quad,
            price_double, price_triple, price_quad,
            released_at, published, created_at, modified_at, created_by
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
        )
    `)
}

func GetDataPackage(prodExistingUmrahDB *sql.DB) (*sql.Rows, error) {
	return prodExistingUmrahDB.Query(`
        SELECT id, travel_id, departure_airline_id, arrival_airline_id,
               name, slug, image, type, share_desc, term_condition,
               facility, currency, dp_type, dp_amount, fee_type,
               fee_amount, soft_delete, created_at, updated_at,
               departure_date, arrival_date, price_double, price_triple,
               price_quad, closed
        FROM td_package 
        WHERE soft_delete = false
        AND departure_date >= '2025-01-10'
    `)
}

func GetPackageItineraryStmt(prodExistingUmrahDB *sql.DB) (*sql.Stmt, error) {
	return prodExistingUmrahDB.Prepare(`
        SELECT i.time, i.activity, i.city_id, i.created_at,
               c.name as city_name
        FROM td_package_itinerary i
        JOIN td_city c ON i.city_id = c.id
        WHERE i.package_id = $1 
        AND i.soft_delete = false
        ORDER BY i.created_at DESC
    `)
}

func InsertItineraryStmt(localUmrahDB *sql.DB) (*sql.Stmt, error) {
	return localUmrahDB.Prepare(`
        INSERT INTO package_itinerary (
            package_id, agenda, created_at, modified_at, created_by
        ) VALUES (
            $1, $2, $3, $4, $5
        )
    `)
}
