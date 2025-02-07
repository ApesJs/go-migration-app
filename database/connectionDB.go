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
		config.LocalDBHost, config.LocalDBPort, config.LocalDBUser, config.LocalDBPassword, config.LocalIdentityDBName)

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
		config.LocalDBHost, config.LocalDBPort, config.LocalDBUser, config.LocalDBPassword, config.LocalUmrahDBName)

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

func ConnectionLocalGeneralDB() *sql.DB {
	config, err := configApp.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	localGeneralConnStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		config.LocalDBHost, config.LocalDBPort, config.LocalDBUser, config.LocalDBPassword, config.LocalGeneralDBName)

	LocalGeneralDB, err := sql.Open("postgres", localGeneralConnStr)
	if err != nil {
		log.Fatal("Error connecting to local general database:", err)
	}

	if err := LocalGeneralDB.Ping(); err != nil {
		log.Fatal("Error connecting to local general database:", err)
	}

	fmt.Println("Successfully connected to local general databases")

	return LocalGeneralDB
}

func ConnectionDevIdentityDB() *sql.DB {
	// Load konfigurasi dari file .env
	config, err := configApp.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration ConnectionDevIdentityDB: %v", err)
	}

	// Koneksi ke database sumber dan target (kode koneksi tetap sama)
	devIdentityConnStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		config.DevDBHost, config.DevDBPort, config.DevDBUser, config.DevDBPassword, config.DevIdentityDBName)

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
		log.Fatalf("Failed to load configuration ConnectionDevUmrahDB: %v", err)
	}

	devUmrahConnStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		config.DevDBHost, config.DevDBPort, config.DevDBUser, config.DevDBPassword, config.DevUmrahDBName)

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

func ConnectionDevGeneralDB() *sql.DB {
	config, err := configApp.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	devGeneralConnStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		config.DevDBHost, config.DevDBPort, config.DevDBUser, config.DevDBPassword, config.DevGeneralDBName)

	devGeneralDB, err := sql.Open("postgres", devGeneralConnStr)
	if err != nil {
		log.Fatal("Error connecting to dev general database:", err)
	}

	if err := devGeneralDB.Ping(); err != nil {
		log.Fatal("Error connecting to dev general database:", err)
	}

	fmt.Println("Successfully connected to dev general databases")

	return devGeneralDB
}

func ConnectionProdExistingUmrahDB() *sql.DB {
	config, err := configApp.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration ConnectionProdExistingUmrahDB: %v", err)
	}

	prodExistingUmrahConnStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		config.ProdExistingDBHost, config.ProdExistingDBPort, config.ProdExistingDBUser, config.ProdExistingDBPassword, config.ProdExistingDBName)

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

func ConnectionProdIdentityDB() *sql.DB {
	config, err := configApp.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	prodIdentityConnStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		config.ProdDBHost, config.ProdDBPort, config.ProdDBUser, config.ProdDBPassword, config.ProdIdentityDBName)

	prodIdentityDB, err := sql.Open("postgres", prodIdentityConnStr)
	if err != nil {
		log.Fatal("Error connecting to prod identity database:", err)
	}

	if err := prodIdentityDB.Ping(); err != nil {
		log.Fatal("Error connecting to prod identity database:", err)
	}

	fmt.Println("Successfully connected to prod identity databases")

	return prodIdentityDB
}

func ConnectionProdUmrahDB() *sql.DB {
	config, err := configApp.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	prodUmrahConnStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		config.ProdDBHost, config.ProdDBPort, config.ProdDBUser, config.ProdDBPassword, config.ProdUmrahDBName)

	prodUmrahDB, err := sql.Open("postgres", prodUmrahConnStr)
	if err != nil {
		log.Fatal("Error connecting to prod umrah database:", err)
	}

	if err := prodUmrahDB.Ping(); err != nil {
		log.Fatal("Error connecting to prod umrah database:", err)
	}

	fmt.Println("Successfully connected to prod umrah databases")

	return prodUmrahDB
}
