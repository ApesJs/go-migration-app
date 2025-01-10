package main

import (
	"github.com/ApesJs/go-migration-app/service/user"
	_ "github.com/lib/pq"
)

func main() {
	//user.BDMService()
	//user.BdmPersonaService()
	//user.UserService()
	user.WukalaPersonaService()
	//user.UserPersonaService()
	//user.CheckingWukalaService()
	//user.MakeUCService()

	//travel.OrganizationService()
	//travel.OrganizationInstanceService()
	//travel.OrganizationUserService()

	//_package.PackageService()
	//hotel.HotelService()
	//airport.AirportService()
	//airline.AirlineService()
}
