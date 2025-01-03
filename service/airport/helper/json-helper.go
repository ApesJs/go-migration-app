package helper

type AirportJSON struct {
	Name     string `json:"name"`
	Code     string `json:"code"`
	City     string `json:"city"`
	Location string `json:"location"`
	Type     string `json:"type"`
}

type ProvinceJSON struct {
	Name      string  `json:"name"`
	Latitude  float32 `json:"latitude"`
	Longitude float32 `json:"longitude"`
	Kode      string  `json:"kode"`
}

type CityJSON struct {
	Name      string  `json:"name"`
	Latitude  float32 `json:"latitude"`
	Longitude float32 `json:"longitude"`
	Kode      string  `json:"kode"`
}
