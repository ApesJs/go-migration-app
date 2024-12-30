package main

import (
	"github.com/ApesJs/go-migration-app/service/package"
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
	//service.GenerateDummyData()
	_package.PackageService()
}
