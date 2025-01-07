package main

import (
	"github.com/ApesJs/go-migration-app/service/user"
	_ "github.com/lib/pq"
)

func main() {
	//service.BDMService()
	//user.UserService()
	user.WukalaPersonaService()
	//service.BdmPersonaService()
	//service.ChangeCityIDPersonaService()
	//service.MakeUCService()

	//service.OrganizationService()
	//service.OrganizationInstanceService()
	//service.OrganizationUserService()

	//_package.PackageService()
	//hotel.HotelService()
	//airport.AirportService()
	//airline.AirlineService()
}
