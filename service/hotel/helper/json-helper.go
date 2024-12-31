package helper

import "time"

// Struktur untuk Hotel JSON yang ada di package
type PackageHotelJSON struct {
	ID         int       `json:"id"`
	Logo       string    `json:"logo"`
	Name       string    `json:"name"`
	CityID     any       `json:"cityId"`
	Rating     int       `json:"rating"`
	Address    string    `json:"address"`
	CityName   string    `json:"cityName"`
	CreatedAt  time.Time `json:"createdAt"`
	CreatedBy  string    `json:"createdBy"`
	ModifiedAt time.Time `json:"modifiedAt"`
	ModifiedBy *string   `json:"modifiedBy"`
}
