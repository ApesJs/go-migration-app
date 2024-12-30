package helper

import "time"

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
