package helper

import (
	"database/sql"
	"time"
)

// Struktur untuk Itinerary
type Activity struct {
	Time     string `json:"time"`
	Activity string `json:"activity"`
	Location string `json:"location"`
}

type AgendaItem struct {
	Title      string     `json:"title"`
	Activities []Activity `json:"activities"`
}

// HotelJSON struktur untuk JSON hotel
type HotelJSON struct {
	ID         int       `json:"id"`
	Logo       string    `json:"logo"`
	Name       string    `json:"name"`
	CityID     string    `json:"cityId"`
	Rating     int       `json:"rating"`
	Address    string    `json:"address"`
	CityName   string    `json:"cityName"`
	CreatedAt  time.Time `json:"createdAt"`
	CreatedBy  string    `json:"createdBy"`
	ModifiedAt time.Time `json:"modifiedAt"`
	ModifiedBy *string   `json:"modifiedBy"`
}

// Struktur untuk JSON airline
type AirportJSON struct {
	ID          int       `json:"id"`
	Code        string    `json:"code"`
	Name        string    `json:"name"`
	CityID      string    `json:"cityId"`
	CityName    string    `json:"cityName"`
	CountryID   string    `json:"countryId"`
	CreatedAt   time.Time `json:"createdAt"`
	CreatedBy   string    `json:"createdBy"`
	ModifiedAt  time.Time `json:"modifiedAt"`
	ModifiedBy  *string   `json:"modifiedBy"`
	CountryName string    `json:"countryName"`
}

type AirlineJSON struct {
	ID          int       `json:"id"`
	Code        string    `json:"code"`
	Logo        string    `json:"logo"`
	Name        string    `json:"name"`
	CountryID   string    `json:"countryId"`
	CreatedAt   time.Time `json:"createdAt"`
	CreatedBy   string    `json:"createdBy"`
	ModifiedAt  time.Time `json:"modifiedAt"`
	ModifiedBy  *string   `json:"modifiedBy"`
	CountryName string    `json:"countryName"`
}

type AirportWrapperJSON struct {
	Airport   AirportJSON `json:"airport"`
	AirportID int         `json:"airportId"`
}

type FlightJSON struct {
	To        AirportWrapperJSON `json:"to"`
	From      AirportWrapperJSON `json:"from"`
	Airline   AirlineJSON        `json:"airline"`
	AirlineID int                `json:"airlineId"`
}

// Struct untuk menyimpan data travel yang tidak memiliki organization_instance
type MissingOrgInstance struct {
	TravelID   string
	TravelName string
}

// Function helper untuk membuat airport JSON
func CreateAirportJSON(id int, code, name, cityId, cityName, countryId string, createdAt, modifiedAt time.Time, createdBy string, modifiedBy *string, countryName string) AirportJSON {
	return AirportJSON{
		ID:          id,
		Code:        code,
		Name:        name,
		CityID:      cityId,
		CityName:    cityName,
		CountryID:   countryId,
		CreatedAt:   createdAt,
		CreatedBy:   createdBy,
		ModifiedAt:  modifiedAt,
		ModifiedBy:  modifiedBy,
		CountryName: countryName,
	}
}

var departureCreatedBy = "643aaa6d-7caa-4c3c-99b5-d062447c3d3a"

func CreateDepartureJSON(airlineCode string, airlineLogo sql.NullString, airlineName string, airlineCreatedAt time.Time, airlineUpdatedAt time.Time, airlineStmt *sql.Stmt, arrivalAirlineID string) (FlightJSON, error) {
	departureFlight := FlightJSON{
		To: AirportWrapperJSON{
			Airport: CreateAirportJSON(
				6,                              // id
				"JED",                          // code
				"Internasional King Abdulaziz", // name
				"0213",                         // cityId
				"JEDDAH",                       // cityName
				"682",                          // countryId
				time.Date(2024, 12, 28, 16, 35, 56, 423000000, time.UTC), // createdAt
				time.Date(2024, 12, 28, 16, 35, 56, 475415000, time.UTC), // modifiedAt
				departureCreatedBy, // createdBy
				nil,                // modifiedBy
				"JEDDAH",           // countryName
			),
			AirportID: 6,
		},
		From: AirportWrapperJSON{
			Airport: CreateAirportJSON(
				3,                        // id
				"SOE",                    // code
				"Soekarno Hatta",         // name
				"3674",                   // cityId
				"KOTA TANGERANG SELATAN", // cityName
				"360",                    // countryId
				time.Date(2024, 10, 31, 9, 10, 3, 359000000, time.UTC), // createdAt
				time.Date(2024, 11, 2, 16, 8, 7, 18000000, time.UTC),   // modifiedAt
				departureCreatedBy,  // createdBy
				&departureCreatedBy, // modifiedBy
				"INDONESIA",         // countryName
			),
			AirportID: 3,
		},
		Airline: AirlineJSON{
			ID:          1,
			Code:        airlineCode,
			Logo:        airlineLogo.String,
			Name:        airlineName,
			CountryID:   "Tidak Ditemukan",
			CreatedAt:   airlineCreatedAt,
			CreatedBy:   "migration",
			ModifiedAt:  airlineUpdatedAt,
			ModifiedBy:  nil,
			CountryName: "Tidak Ditemukan",
		},
		AirlineID: 1,
	}

	// Get airline data for arrival
	err := airlineStmt.QueryRow(arrivalAirlineID).Scan(
		&airlineCode,
		&airlineLogo,
		&airlineName,
		&airlineCreatedAt,
		&airlineUpdatedAt,
	)

	return departureFlight, err
}

func CreateArrivalJSON(airlineCode string, airlineLogo sql.NullString, airlineName string, airlineCreatedAt time.Time, airlineUpdatedAt time.Time) FlightJSON {
	arrivalFlight := FlightJSON{
		To: AirportWrapperJSON{
			Airport: CreateAirportJSON(
				3,                        // id
				"SOE",                    // code
				"Soekarno Hatta",         // name
				"3674",                   // cityId
				"KOTA TANGERANG SELATAN", // cityName
				"360",                    // countryId
				time.Date(2024, 10, 31, 9, 10, 3, 359000000, time.UTC), // createdAt
				time.Date(2024, 11, 2, 16, 8, 7, 18000000, time.UTC),   // modifiedAt
				departureCreatedBy,  // createdBy
				&departureCreatedBy, // modifiedBy
				"INDONESIA",         // countryName
			),
			AirportID: 3,
		},
		From: AirportWrapperJSON{
			Airport: CreateAirportJSON(
				6,                              // id
				"JED",                          // code
				"Internasional King Abdulaziz", // name
				"0213",                         // cityId
				"JEDDAH",                       // cityName
				"682",                          // countryId
				time.Date(2024, 12, 28, 16, 35, 56, 423000000, time.UTC), // createdAt
				time.Date(2024, 12, 28, 16, 35, 56, 475415000, time.UTC), // modifiedAt
				departureCreatedBy, // createdBy
				nil,                // modifiedBy
				"JEDDAH",           // countryName
			),
			AirportID: 6,
		},
		Airline: AirlineJSON{
			ID:          1,
			Code:        airlineCode,
			Logo:        airlineLogo.String,
			Name:        airlineName,
			CountryID:   "Tidak Ditemukan",
			CreatedAt:   airlineCreatedAt,
			CreatedBy:   "migration",
			ModifiedAt:  airlineUpdatedAt,
			ModifiedBy:  nil,
			CountryName: "Tidak Ditemukan",
		},
		AirlineID: 1,
	}

	return arrivalFlight
}
