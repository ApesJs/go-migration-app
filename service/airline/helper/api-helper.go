package helper

import (
	"encoding/json"
	"os"
)

func ReadAirlineJSON(filename string) ([]Airline, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var airlines []Airline
	err = json.Unmarshal(data, &airlines)
	if err != nil {
		return nil, err
	}

	return airlines, nil
}

// ProcessAirlineData menambahkan country info ke data airline
func ProcessAirlineData(airlines []Airline, countryName, countryID string) []AirlineWithCountry {
	result := make([]AirlineWithCountry, len(airlines))

	for i, airline := range airlines {
		result[i] = AirlineWithCountry{
			Name:        airline.Name,
			Code:        airline.Code,
			CountryName: countryName,
			CountryID:   countryID,
		}
	}

	return result
}
