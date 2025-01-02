package helper

import (
	"database/sql"
	"fmt"
)

func ProcessAirport(tx *sql.Tx, airport AirportJSON,
	getCityIDStmt, checkAirportExistStmt, insertAirportStmt *sql.Stmt) (int, error) {

	// Validasi data airport
	if airport.Name == "" {
		return 0, fmt.Errorf("airport name is empty")
	}
	if airport.Code == "" {
		return 0, fmt.Errorf("airport code is empty")
	}
	if airport.City == "" {
		return 0, fmt.Errorf("city name is empty")
	}

	// Check if airport already exists
	var existingID int
	err := checkAirportExistStmt.QueryRow(airport.Code).Scan(&existingID)
	if err != sql.ErrNoRows {
		if err != nil {
			return 0, fmt.Errorf("error checking airport existence: %v", err)
		}
		return 0, nil // Airport already exists
	}

	// Get correct city_id from location_city based on cityName
	var cityID int
	err = getCityIDStmt.QueryRow(airport.City).Scan(&cityID)
	if err != nil {
		return 0, fmt.Errorf("error getting city ID for city '%s': %v", airport.City, err)
	}

	// Insert new airport with correct city_id
	var newID int
	err = insertAirportStmt.QueryRow(
		airport.Name,
		airport.Code,
		"INDONESIA", // country_name for Indonesia
		360,         // country_id for Indonesia
		airport.City,
		cityID,
		"migration", // created_by
		nil,         // modified_by
	).Scan(&newID)

	if err != nil {
		return 0, fmt.Errorf("error inserting airport: %v", err)
	}

	return newID, nil
}
