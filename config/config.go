package config

import (
	"fmt"
	"github.com/joho/godotenv"
	"os"
)

type Config struct {
	LocalIdentityDBHost         string
	LocalIdentityDBPort         string
	LocalIdentityDBName         string
	LocalIdentityDBUser         string
	LocalIdentityDBPassword     string
	LocalUmrahDBHost            string
	LocalUmrahDBPort            string
	LocalUmrahDBName            string
	LocalUmrahDBUser            string
	LocalUmrahDBPassword        string
	DevIdentityDBHost           string
	DevIdentityDBPort           string
	DevIdentityDBName           string
	DevIdentityDBUser           string
	DevIdentityDBPassword       string
	DevUmrahDBHost              string
	DevUmrahDBPort              string
	DevUmrahDBName              string
	DevUmrahDBUser              string
	DevUmrahDBPassword          string
	ProdExistingUmrahDBHost     string
	ProdExistingUmrahDBPort     string
	ProdExistingUmrahDBName     string
	ProdExistingUmrahDBUser     string
	ProdExistingUmrahDBPassword string
}

func LoadConfig() (Config, error) {
	if err := godotenv.Load(); err != nil {
		return Config{}, fmt.Errorf("error loading .env file: %v", err)
	}

	config := Config{
		LocalIdentityDBHost:         os.Getenv("LOCAL_IDENTITY_DB_HOST"),
		LocalIdentityDBPort:         os.Getenv("LOCAL_IDENTITY_DB_PORT"),
		LocalIdentityDBName:         os.Getenv("LOCAL_IDENTITY_DB_NAME"),
		LocalIdentityDBUser:         os.Getenv("LOCAL_IDENTITY_DB_USER"),
		LocalIdentityDBPassword:     os.Getenv("LOCAL_IDENTITY_DB_PASSWORD"),
		LocalUmrahDBHost:            os.Getenv("LOCAL_UMRAH_DB_HOST"),
		LocalUmrahDBPort:            os.Getenv("LOCAL_UMRAH_DB_PORT"),
		LocalUmrahDBName:            os.Getenv("LOCAL_UMRAH_DB_NAME"),
		LocalUmrahDBUser:            os.Getenv("LOCAL_UMRAH_DB_USER"),
		LocalUmrahDBPassword:        os.Getenv("LOCAL_UMRAH_DB_PASSWORD"),
		DevIdentityDBHost:           os.Getenv("DEV_IDENTITY_DB_HOST"),
		DevIdentityDBPort:           os.Getenv("DEV_IDENTITY_DB_PORT"),
		DevIdentityDBName:           os.Getenv("DEV_IDENTITY_DB_NAME"),
		DevIdentityDBUser:           os.Getenv("DEV_IDENTITY_DB_USER"),
		DevIdentityDBPassword:       os.Getenv("DEV_IDENTITY_DB_PASSWORD"),
		DevUmrahDBHost:              os.Getenv("DEV_UMRAH_DB_HOST"),
		DevUmrahDBPort:              os.Getenv("DEV_UMRAH_DB_PORT"),
		DevUmrahDBName:              os.Getenv("DEV_UMRAH_DB_NAME"),
		DevUmrahDBUser:              os.Getenv("DEV_UMRAH_DB_USER"),
		DevUmrahDBPassword:          os.Getenv("DEV_UMRAH_DB_PASSWORD"),
		ProdExistingUmrahDBHost:     os.Getenv("PROD_EXISTING_UMRAH_DB_HOST"),
		ProdExistingUmrahDBPort:     os.Getenv("PROD_EXISTING_UMRAH_DB_PORT"),
		ProdExistingUmrahDBName:     os.Getenv("PROD_EXISTING_UMRAH_DB_NAME"),
		ProdExistingUmrahDBUser:     os.Getenv("PROD_EXISTING_UMRAH_DB_USER"),
		ProdExistingUmrahDBPassword: os.Getenv("PROD_EXISTING_UMRAH_DB_PASSWORD"),
	}

	// Validasi konfigurasi
	if config.LocalIdentityDBName == "" || config.LocalIdentityDBUser == "" || config.LocalIdentityDBPassword == "" ||
		config.LocalUmrahDBName == "" || config.LocalUmrahDBUser == "" || config.LocalUmrahDBPassword == "" ||
		config.DevUmrahDBName == "" || config.DevUmrahDBUser == "" || config.DevUmrahDBPassword == "" ||
		config.ProdExistingUmrahDBName == "" || config.ProdExistingUmrahDBUser == "" || config.ProdExistingUmrahDBPassword == "" ||
		config.DevIdentityDBName == "" || config.DevIdentityDBUser == "" || config.DevIdentityDBPassword == "" {
		return Config{}, fmt.Errorf("missing required environment variables")
	}

	return config, nil
}
