package main

import (
	_ "github.com/lib/pq"
	"migration-app3/service"
)

func main() {
	//service.UserService()
	service.BDMService()
}
