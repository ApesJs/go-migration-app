package main

import (
	airport_airline "github.com/ApesJs/go-migration-app/service/airport-airline"
	_ "github.com/lib/pq"
)

func main() {
	//service.UserService()
	//service.BDMService()
	//service.BdmPersonaService()
	//service.ChangeCityIDPersonaService()
	//service.MakeUCService()
	//service.OrganizationService()
	//service.OrganizationInstanceService()
	//service.OrganizationUserService()
	//_package.PackageService()
	//hotel.HotelService()
	airport_airline.AirportAirlineService()
}
