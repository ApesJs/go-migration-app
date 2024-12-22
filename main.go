package main

import (
	"github.com/ApesJs/go-migration-app/service"
	_ "github.com/lib/pq"
)

func main() {
	//service.UserService()
	//service.BDMService()
	//service.BdmPersonaService()
	//service.ChangeCityIDPersonaService()
	//service.MakeUCService()
	service.TravelService()
}
