package config

import (
	"fmt"
	"github.com/joho/godotenv"
	"os"
)

type Config struct {
	LocalIdentityDBName string
	LocalUmrahDBName    string
	LocalGeneralDBName  string
	LocalDBHost         string
	LocalDBPort         string
	LocalDBUser         string
	LocalDBPassword     string

	DevIdentityDBName string
	DevUmrahDBName    string
	DevGeneralDBName  string
	DevDBHost         string
	DevDBPort         string
	DevDBUser         string
	DevDBPassword     string

	ProdIdentityDBName string
	ProdUmrahDBName    string
	ProdGeneralDBName  string
	ProdDBHost         string
	ProdDBPort         string
	ProdDBUser         string
	ProdDBPassword     string

	ProdExistingDBName     string
	ProdExistingDBHost     string
	ProdExistingDBPort     string
	ProdExistingDBUser     string
	ProdExistingDBPassword string
}

func LoadConfig() (Config, error) {
	if err := godotenv.Load(); err != nil {
		return Config{}, fmt.Errorf("error loading .env file: %v", err)
	}

	config := Config{
		LocalIdentityDBName: os.Getenv("LOCAL_IDENTITY_DB_NAME"),
		LocalUmrahDBName:    os.Getenv("LOCAL_UMRAH_DB_NAME"),
		LocalGeneralDBName:  os.Getenv("LOCAL_GENERAL_DB_NAME"),
		LocalDBHost:         os.Getenv("LOCAL_DB_HOST"),
		LocalDBPort:         os.Getenv("LOCAL_DB_PORT"),
		LocalDBUser:         os.Getenv("LOCAL_DB_USER"),
		LocalDBPassword:     os.Getenv("LOCAL_DB_PASSWORD"),

		DevIdentityDBName: os.Getenv("DEV_IDENTITY_DB_NAME"),
		DevUmrahDBName:    os.Getenv("DEV_UMRAH_DB_NAME"),
		DevGeneralDBName:  os.Getenv("DEV_GENERAL_DB_NAME"),
		DevDBHost:         os.Getenv("DEV_DB_HOST"),
		DevDBPort:         os.Getenv("DEV_DB_PORT"),
		DevDBUser:         os.Getenv("DEV_DB_USER"),
		DevDBPassword:     os.Getenv("DEV_DB_PASSWORD"),

		ProdIdentityDBName: os.Getenv("PROD_IDENTITY_DB_NAME"),
		ProdUmrahDBName:    os.Getenv("PROD_UMRAH_DB_NAME"),
		ProdGeneralDBName:  os.Getenv("PROD_GENERAL_DB_NAME"),
		ProdDBHost:         os.Getenv("PROD_DB_HOST"),
		ProdDBPort:         os.Getenv("PROD_DB_PORT"),
		ProdDBUser:         os.Getenv("PROD_DB_USER"),
		ProdDBPassword:     os.Getenv("PROD_DB_PASSWORD"),

		ProdExistingDBName:     os.Getenv("PROD_EXISTING_DB_NAME"),
		ProdExistingDBHost:     os.Getenv("PROD_EXISTING_DB_HOST"),
		ProdExistingDBPort:     os.Getenv("PROD_EXISTING_DB_PORT"),
		ProdExistingDBUser:     os.Getenv("PROD_EXISTING_DB_USER"),
		ProdExistingDBPassword: os.Getenv("PROD_EXISTING_DB_PASSWORD"),
	}

	// Validasi konfigurasi
	if config.LocalIdentityDBName == "" || config.LocalUmrahDBName == "" || config.LocalGeneralDBName == "" || config.LocalDBHost == "" || config.LocalDBPort == "" || config.LocalDBUser == "" || config.LocalDBPassword == "" ||
		config.DevIdentityDBName == "" || config.DevUmrahDBName == "" || config.DevGeneralDBName == "" || config.DevDBHost == "" || config.DevDBPort == "" || config.DevDBUser == "" || config.DevDBPassword == "" ||
		config.ProdIdentityDBName == "" || config.ProdUmrahDBName == "" || config.ProdGeneralDBName == "" || config.ProdDBHost == "" || config.ProdDBPort == "" || config.ProdDBUser == "" || config.ProdDBPassword == "" ||
		config.ProdExistingDBName == "" || config.ProdExistingDBHost == "" || config.ProdExistingDBPort == "" || config.ProdExistingDBUser == "" || config.ProdExistingDBPassword == "" {
		return Config{}, fmt.Errorf("missing required environment variables")
	}

	return config, nil
}
