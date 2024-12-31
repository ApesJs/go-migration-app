package main

import (
	"github.com/ApesJs/go-migration-app/service/hotel"
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
	hotel.HotelService()
}
