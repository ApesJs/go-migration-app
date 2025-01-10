package user

import (
	"fmt"
	"github.com/ApesJs/go-migration-app/database"
	"github.com/ApesJs/go-migration-app/service/user/helper"
	"log"
	"time"
)

func UserService() {
	// Koneksi Database
	prodExistingUmrahDB := database.ConnectionProdExistingUmrahDB()
	defer prodExistingUmrahDB.Close()

	//devIdentityDB := database.ConnectionDevIdentityDB()
	//defer devIdentityDB.Close()

	localIdentityDB := database.ConnectionLocalIdentityDB()
	defer localIdentityDB.Close()

	// Cek dan buat role wukala jika belum ada
	err := helper.EnsureWukalaRole(localIdentityDB)
	if err != nil {
		log.Fatal("Error ensuring wukala role:", err)
	}

	// Menghitung total records
	totalRows, totalTravelAgents, err := helper.CountTotalRecords(prodExistingUmrahDB)
	if err != nil {
		log.Fatal("Error counting records:", err)
	}

	fmt.Printf("Found %d total records to transfer\n", totalRows)
	fmt.Printf("Found %d wukala in source database\n", totalTravelAgents)

	// Membuat progress bar
	bar := helper.CreateProgressBar(totalRows)

	// Prepare statements
	stmts, err := helper.PrepareStatements(prodExistingUmrahDB, localIdentityDB)
	if err != nil {
		log.Fatal("Error preparing statements:", err)
	}
	defer stmts.CloseAll()

	// Begin transaction
	tx, err := localIdentityDB.Begin()
	if err != nil {
		log.Fatal("Error starting transaction:", err)
	}

	// Prepare statements dalam transaksi
	txStmts := helper.CreateTxStatements(tx, stmts)

	startTime := time.Now()

	// Transfer data
	stats := helper.TransferData(prodExistingUmrahDB, txStmts, bar)

	// Commit transaction
	if err = tx.Commit(); err != nil {
		log.Printf("Error committing transaction: %v", err)
		tx.Rollback()
		return
	}

	duration := time.Since(startTime)

	// Print summary
	helper.PrintSummary(stats, totalRows, totalTravelAgents, duration)
}
