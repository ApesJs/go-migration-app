package helper

import (
	"database/sql"
	"fmt"
)

func ProcessAirportIndo(tx *sql.Tx, airport AirportJSON,
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
		"INDONESIA",
		360,
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
		"SAUDI ARABIA", // country_name for Saudi Arabia
		682,            // country_id for Saudi Arabia
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

func ProcessProvince(tx *sql.Tx, province ProvinceJSON,
	checkProvinceExistStmt, insertProvinceStmt *sql.Stmt) (string, error) {

	// Validasi data province
	if province.Name == "" {
		return "", fmt.Errorf("province name is empty")
	}
	if province.Kode == "" {
		return "", fmt.Errorf("province code is empty")
	}

	// Check if province already exists
	var existingID string
	err := checkProvinceExistStmt.QueryRow(province.Kode).Scan(&existingID)
	if err != sql.ErrNoRows {
		if err != nil {
			return "", fmt.Errorf("error checking province existence: %v", err)
		}
		return "", nil // Province already exists
	}

	// Insert new province
	var newID string
	err = insertProvinceStmt.QueryRow(
		province.Kode, // id
		"682",         // country_id for Saudi Arabia
		province.Name, // name
		province.Name, // alt_name
		province.Latitude,
		province.Longitude,
	).Scan(&newID)

	if err != nil {
		return "", fmt.Errorf("error inserting province: %v", err)
	}

	return newID, nil
}

func ProcessCity(tx *sql.Tx, city CityJSON,
	checkCityExistStmt, insertCityStmt *sql.Stmt) (string, error) {

	// Validasi data city
	if city.Name == "" {
		return "", fmt.Errorf("city name is empty")
	}
	if city.Kode == "" {
		return "", fmt.Errorf("city code is empty")
	}

	// Check if city already exists
	var existingID string
	err := checkCityExistStmt.QueryRow(city.Kode).Scan(&existingID)
	if err != sql.ErrNoRows {
		if err != nil {
			return "", fmt.Errorf("error checking city existence: %v", err)
		}
		return "", nil // City already exists
	}

	// Get province_id from city code (2 first digits)
	provinceID := city.Kode[:2]

	// Insert new city
	var newID string
	err = insertCityStmt.QueryRow(
		city.Kode,  // id
		city.Name,  // name
		provinceID, // province_id (2 first digits from kode)
		city.Name,  // alt_name
		city.Latitude,
		city.Longitude,
	).Scan(&newID)

	if err != nil {
		return "", fmt.Errorf("error inserting city: %v", err)
	}

	return newID, nil
}
