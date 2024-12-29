package config

import (
	"fmt"
	"github.com/joho/godotenv"
	"os"
)

type Config struct {
	SourceDBHost       string
	SourceDBPort       string
	SourceDBName       string
	SourceDBUser       string
	SourceDBPassword   string
	TargetDBHost       string
	TargetDBPort       string
	TargetDBName       string
	TargetDBUser       string
	TargetDBPassword   string
	IdentityDBHost     string
	IdentityDBPort     string
	IdentityDBName     string
	IdentityDBUser     string
	IdentityDBPassword string
}

func LoadConfig() (Config, error) {
	if err := godotenv.Load(); err != nil {
		return Config{}, fmt.Errorf("error loading .env file: %v", err)
	}

	config := Config{
		SourceDBHost:       os.Getenv("SOURCE_DB_HOST"),
		SourceDBPort:       os.Getenv("SOURCE_DB_PORT"),
		SourceDBName:       os.Getenv("SOURCE_DB_NAME"),
		SourceDBUser:       os.Getenv("SOURCE_DB_USER"),
		SourceDBPassword:   os.Getenv("SOURCE_DB_PASSWORD"),
		TargetDBHost:       os.Getenv("TARGET_DB_HOST"),
		TargetDBPort:       os.Getenv("TARGET_DB_PORT"),
		TargetDBName:       os.Getenv("TARGET_DB_NAME"),
		TargetDBUser:       os.Getenv("TARGET_DB_USER"),
		TargetDBPassword:   os.Getenv("TARGET_DB_PASSWORD"),
		IdentityDBHost:     os.Getenv("IDENTITY_DB_HOST"),
		IdentityDBPort:     os.Getenv("IDENTITY_DB_PORT"),
		IdentityDBName:     os.Getenv("IDENTITY_DB_NAME"),
		IdentityDBUser:     os.Getenv("IDENTITY_DB_USER"),
		IdentityDBPassword: os.Getenv("IDENTITY_DB_PASSWORD"),
	}

	// Validasi konfigurasi
	if config.SourceDBName == "" || config.SourceDBUser == "" || config.SourceDBPassword == "" ||
		config.TargetDBName == "" || config.TargetDBUser == "" || config.TargetDBPassword == "" ||
		config.IdentityDBName == "" || config.IdentityDBUser == "" || config.IdentityDBPassword == "" {
		return Config{}, fmt.Errorf("missing required environment variables")
	}

	return config, nil
}
