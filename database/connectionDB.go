package database

import (
	"database/sql"
	"fmt"
	configApp "github.com/ApesJs/go-migration-app/config"
	"log"
)

func ConnectionLocalIdentityDB() *sql.DB {
	config, err := configApp.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	localIdentityConnStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		config.LocalIdentityDBHost, config.LocalIdentityDBPort, config.LocalIdentityDBUser, config.LocalIdentityDBPassword, config.LocalIdentityDBName)

	LocalIdentityDB, err := sql.Open("postgres", localIdentityConnStr)
	if err != nil {
		log.Fatal("Error connecting to local identity database:", err)
	}

	if err := LocalIdentityDB.Ping(); err != nil {
		log.Fatal("Error connecting to local identity database:", err)
	}

	fmt.Println("Successfully connected to local identity databases")

	return LocalIdentityDB
}

func ConnectionLocalUmrahDB() *sql.DB {
	config, err := configApp.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	localUmrahConnStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		config.LocalUmrahDBHost, config.LocalUmrahDBPort, config.LocalUmrahDBUser, config.LocalUmrahDBPassword, config.LocalUmrahDBName)

	LocalUmrahDB, err := sql.Open("postgres", localUmrahConnStr)
	if err != nil {
		log.Fatal("Error connecting to local umrah database:", err)
	}

	if err := LocalUmrahDB.Ping(); err != nil {
		log.Fatal("Error connecting to local umrah database:", err)
	}

	fmt.Println("Successfully connected to local umrah databases")

	return LocalUmrahDB
}

func ConnectionDevIdentityDB() *sql.DB {
	// Load konfigurasi dari file .env
	config, err := configApp.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Koneksi ke database sumber dan target (kode koneksi tetap sama)
	devIdentityConnStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		config.DevIdentityDBHost, config.DevIdentityDBPort, config.DevIdentityDBUser, config.DevIdentityDBPassword, config.DevIdentityDBName)

	devIdentityDB, err := sql.Open("postgres", devIdentityConnStr)
	if err != nil {
		log.Fatal("Error connecting to dev identity database:", err)
	}

	// Test koneksi kedua database
	if err := devIdentityDB.Ping(); err != nil {
		log.Fatal("Error connecting to dev identity database:", err)
	}

	fmt.Println("Successfully connected to dev identity databases")

	return devIdentityDB
}

func ConnectionDevUmrahDB() *sql.DB {
	config, err := configApp.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	devUmrahConnStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		config.DevUmrahDBHost, config.DevUmrahDBPort, config.DevUmrahDBUser, config.DevUmrahDBPassword, config.DevUmrahDBName)

	devUmrahDB, err := sql.Open("postgres", devUmrahConnStr)
	if err != nil {
		log.Fatal("Error connecting to dev umrah database:", err)
	}

	if err := devUmrahDB.Ping(); err != nil {
		log.Fatal("Error connecting to dev umrah database:", err)
	}

	fmt.Println("Successfully connected to dev umrah databases")

	return devUmrahDB
}

func ConnectionProdExistingUmrahDB() *sql.DB {
	config, err := configApp.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	prodExistingUmrahConnStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		config.ProdExistingUmrahDBHost, config.ProdExistingUmrahDBPort, config.ProdExistingUmrahDBUser, config.ProdExistingUmrahDBPassword, config.ProdExistingUmrahDBName)

	prodExistingUmrahDB, err := sql.Open("postgres", prodExistingUmrahConnStr)
	if err != nil {
		log.Fatal("Error connecting to prod existing umrah database:", err)
	}

	if err := prodExistingUmrahDB.Ping(); err != nil {
		log.Fatal("Error connecting to prod existing umrah database:", err)
	}

	fmt.Println("Successfully connected to prod existing umrah databases")

	return prodExistingUmrahDB
}
