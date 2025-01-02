package helper

import (
	"database/sql"
	"fmt"
)

func ProcessHotel(tx *sql.Tx, hotel PackageHotelJSON,
	getCityIDStmt, checkHotelExistStmt, insertHotelStmt *sql.Stmt) (int, error) {

	// Validasi data hotel
	if hotel.Name == "" {
		return 0, fmt.Errorf("hotel name is empty")
	}
	if hotel.CityName == "" {
		return 0, fmt.Errorf("city name is empty")
	}

	// Check if hotel already exists
	var existingID int
	err := checkHotelExistStmt.QueryRow(hotel.Name).Scan(&existingID)
	if err != sql.ErrNoRows {
		if err != nil {
			return 0, fmt.Errorf("error checking hotel existence: %v", err)
		}
		return 0, nil // Hotel already exists
	}

	// Get correct city_id from location_city based on cityName
	var cityID int
	err = getCityIDStmt.QueryRow(hotel.CityName).Scan(&cityID)
	if err != nil {
		return 0, fmt.Errorf("error getting city ID for city '%s': %v", hotel.CityName, err)
	}

	// Jika address kosong, gunakan default value
	address := hotel.Address
	if address == "" {
		address = "Tidak Ada"
	}

	// Insert new hotel with correct city_id
	var newID int
	err = insertHotelStmt.QueryRow(
		hotel.Name,
		address,
		hotel.CityName,
		cityID,
		hotel.Rating,
		hotel.Logo,
		hotel.CreatedAt,
		hotel.ModifiedAt,
		"migration",
		hotel.ModifiedBy,
	).Scan(&newID)

	if err != nil {
		return 0, fmt.Errorf("error inserting hotel: %v", err)
	}

	return newID, nil
}
