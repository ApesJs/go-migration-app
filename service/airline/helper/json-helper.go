package helper

type Airline struct {
	Name string `json:"name"`
	Code string `json:"code"`
}

type AirlineWithCountry struct {
	Name        string
	Code        string
	CountryName string
	CountryID   string
}
