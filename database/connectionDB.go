package database

import (
	"database/sql"
	"fmt"
	configApp "github.com/ApesJs/go-migration-app/config"
	"log"
)

func ConnectionDB() (*sql.DB, *sql.DB) {
	// Load konfigurasi dari file .env
	config, err := configApp.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Koneksi ke database sumber dan target (kode koneksi tetap sama)
	sourceConnStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		config.SourceDBHost, config.SourceDBPort, config.SourceDBUser, config.SourceDBPassword, config.SourceDBName)

	targetConnStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		config.TargetDBHost, config.TargetDBPort, config.TargetDBUser, config.TargetDBPassword, config.TargetDBName)

	sourceDB, err := sql.Open("postgres", sourceConnStr)
	if err != nil {
		log.Fatal("Error connecting to source database:", err)
	}

	targetDB, err := sql.Open("postgres", targetConnStr)
	if err != nil {
		log.Fatal("Error connecting to target database:", err)
	}

	// Test koneksi kedua database
	if err := sourceDB.Ping(); err != nil {
		log.Fatal("Error connecting to source database:", err)
	}
	if err := targetDB.Ping(); err != nil {
		log.Fatal("Error connecting to target database:", err)
	}

	fmt.Println("Successfully connected to both databases")

	return sourceDB, targetDB
}
