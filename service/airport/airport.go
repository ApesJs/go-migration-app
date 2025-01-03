package airport

import (
	"encoding/json"
	"fmt"
	"github.com/ApesJs/go-migration-app/database"
	"github.com/ApesJs/go-migration-app/service/airport/helper"
	"github.com/schollz/progressbar/v3"
	"log"
	"os"
	"time"
)

func AirportService() {
	// Database connections
	prodExistingUmrahDB := database.ConnectionProdExistingUmrahDB()
	devIdentityDB := database.ConnectionDevIdentityDB()
	devGeneralDB := database.ConnectionDevGeneralDB()
	defer devGeneralDB.Close()
	defer devIdentityDB.Close()
	defer prodExistingUmrahDB.Close()

	fmt.Printf("\nPhase 1: Migrating Airports from JSON\n")
	fmt.Printf("====================================\n")

	// Read JSON file
	jsonFile, err := os.ReadFile("service/airport/seed/airport/airport-indo.json")
	if err != nil {
		log.Fatal("Error reading JSON file:", err)
	}

	// Parse JSON data
	var airportsIndo []helper.AirportJSON
	err = json.Unmarshal(jsonFile, &airportsIndo)
	if err != nil {
		log.Fatal("Error parsing JSON:", err)
	}

	totalAirports := helper.TotalAirports(airportsIndo)
	fmt.Printf("Found %d airports to transfer\n", totalAirports)

	// Prepare statements
	getCityIDStmt, err := helper.GetCityIDFromLocationStmt(devIdentityDB)
	if err != nil {
		log.Fatal("Error preparing get city ID statement:", err)
	}
	defer getCityIDStmt.Close()

	checkAirportExistStmt, err := helper.CheckAirportExistStmt(devGeneralDB)
	if err != nil {
		log.Fatal("Error preparing check airport statement:", err)
	}
	defer checkAirportExistStmt.Close()

	insertAirportStmt, err := helper.InsertAirportStmt(devGeneralDB)
	if err != nil {
		log.Fatal("Error preparing insert airport statement:", err)
	}
	defer insertAirportStmt.Close()

	// Progress bar
	bar := progressbar.NewOptions(totalAirports,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription("[cyan][1/1][reset] Transferring airports..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))

	// Statistics
	var (
		processedCount int
		insertedCount  int
		skippedCount   int
		errorCount     int
	)

	startTime := time.Now()

	// Begin transaction
	tx, err := devGeneralDB.Begin()
	if err != nil {
		log.Fatal("Error starting transaction:", err)
	}

	// Process each airport
	for _, airport := range airportsIndo {
		newID, err := helper.ProcessAirportIndo(tx, airport, getCityIDStmt, checkAirportExistStmt, insertAirportStmt)
		if err != nil {
			log.Printf("Error processing airport %s: %v", airport.Code, err)
			errorCount++
			bar.Add(1)
			continue
		}

		if newID > 0 {
			insertedCount++
		} else {
			skippedCount++
		}
		processedCount++
		bar.Add(1)
	}

	// Commit transaction
	err = tx.Commit()
	if err != nil {
		log.Printf("Error committing transaction: %v", err)
		tx.Rollback()
		return
	}

	// Update progress bar untuk selesai
	bar.Finish()

	duration := time.Since(startTime)

	// Print summary
	fmt.Printf("\nMigration Summary:\n")
	fmt.Printf("=================\n")
	fmt.Printf("Total airports found: %d\n", totalAirports)
	fmt.Printf("Successfully processed airports: %d\n", processedCount)
	fmt.Printf("Successfully inserted airports: %d\n", insertedCount)
	fmt.Printf("Skipped (already exist): %d\n", skippedCount)
	fmt.Printf("Failed transfers: %d\n", errorCount)
	fmt.Printf("\nPerformance Metrics:\n")
	fmt.Printf("Duration: %s\n", duration.Round(time.Second))
	fmt.Printf("Average speed: %.2f airports/second\n", float64(processedCount)/duration.Seconds())

	// Part 1: Migrate Provinces
	fmt.Printf("\nPhase 1.1: Migrating Provinces from JSON\n")
	fmt.Printf("=====================================\n")

	// Read Province JSON file
	provinceFile, err := os.ReadFile("service/airport/seed/airport/airport-location-arab/airport-province-arab.json")
	if err != nil {
		log.Fatal("Error reading province JSON file:", err)
	}

	var provinces []helper.ProvinceJSON
	err = json.Unmarshal(provinceFile, &provinces)
	if err != nil {
		log.Fatal("Error parsing province JSON:", err)
	}

	totalProvinces := helper.TotalProvinces(provinces)
	fmt.Printf("Found %d provinces to transfer\n", totalProvinces)

	// Prepare province statements
	checkProvinceExistStmt, err := helper.CheckProvinceExistStmt(devIdentityDB)
	if err != nil {
		log.Fatal("Error preparing check province statement:", err)
	}
	defer checkProvinceExistStmt.Close()

	insertProvinceStmt, err := helper.InsertProvinceStmt(devIdentityDB)
	if err != nil {
		log.Fatal("Error preparing insert province statement:", err)
	}
	defer insertProvinceStmt.Close()

	// Progress bar for provinces
	provinceBar := progressbar.NewOptions(totalProvinces,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription("[cyan][1/3][reset] Transferring provinces..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))

	var (
		processedProvinces int
		insertedProvinces  int
		skippedProvinces   int
		errorProvinces     int
	)

	startTimeProvinces := time.Now()

	// Begin province transaction
	txProvince, err := devIdentityDB.Begin()
	if err != nil {
		log.Fatal("Error starting province transaction:", err)
	}

	// Process each province
	for _, province := range provinces {
		newID, err := helper.ProcessProvince(txProvince, province, checkProvinceExistStmt, insertProvinceStmt)
		if err != nil {
			log.Printf("Error processing province %s: %v", province.Kode, err)
			errorProvinces++
			provinceBar.Add(1)
			continue
		}

		if newID != "" {
			insertedProvinces++
		} else {
			skippedProvinces++
		}
		processedProvinces++
		provinceBar.Add(1)
	}

	// Commit province transaction
	err = txProvince.Commit()
	if err != nil {
		log.Printf("Error committing province transaction: %v", err)
		txProvince.Rollback()
		return
	}

	durationProvinces := time.Since(startTimeProvinces)

	// Print province summary
	fmt.Printf("\nProvince Migration Summary:\n")
	fmt.Printf("==========================\n")
	fmt.Printf("Total provinces found: %d\n", totalProvinces)
	fmt.Printf("Successfully processed provinces: %d\n", processedProvinces)
	fmt.Printf("Successfully inserted provinces: %d\n", insertedProvinces)
	fmt.Printf("Skipped (already exist): %d\n", skippedProvinces)
	fmt.Printf("Failed transfers: %d\n", errorProvinces)
	fmt.Printf("\nPerformance Metrics:\n")
	fmt.Printf("Duration: %s\n", durationProvinces.Round(time.Second))
	fmt.Printf("Average speed: %.2f provinces/second\n", float64(processedProvinces)/durationProvinces.Seconds())

	// Part 2: Migrate Cities
	fmt.Printf("\nPhase 1.2: Migrating Cities from JSON\n")
	fmt.Printf("===================================\n")

	// Read City JSON file
	cityFile, err := os.ReadFile("service/airport/seed/airport/airport-location-arab/airport-city-arab.json")
	if err != nil {
		log.Fatal("Error reading city JSON file:", err)
	}

	var cities []helper.CityJSON
	err = json.Unmarshal(cityFile, &cities)
	if err != nil {
		log.Fatal("Error parsing city JSON:", err)
	}

	totalCities := helper.TotalCities(cities)
	fmt.Printf("Found %d cities to transfer\n", totalCities)

	// Prepare city statements
	checkCityExistStmt, err := helper.CheckCityExistStmt(devIdentityDB)
	if err != nil {
		log.Fatal("Error preparing check city statement:", err)
	}
	defer checkCityExistStmt.Close()

	insertCityStmt, err := helper.InsertCityStmt(devIdentityDB)
	if err != nil {
		log.Fatal("Error preparing insert city statement:", err)
	}
	defer insertCityStmt.Close()

	// Progress bar for cities
	cityBar := progressbar.NewOptions(totalCities,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription("[cyan][2/3][reset] Transferring cities..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))

	var (
		processedCities int
		insertedCities  int
		skippedCities   int
		errorCities     int
	)

	startTimeCities := time.Now()

	// Begin city transaction
	txCity, err := devIdentityDB.Begin()
	if err != nil {
		log.Fatal("Error starting city transaction:", err)
	}

	// Process each city
	for _, city := range cities {
		newID, err := helper.ProcessCity(txCity, city, checkCityExistStmt, insertCityStmt)
		if err != nil {
			log.Printf("Error processing city %s: %v", city.Kode, err)
			errorCities++
			cityBar.Add(1)
			continue
		}

		if newID != "" {
			insertedCities++
		} else {
			skippedCities++
		}
		processedCities++
		cityBar.Add(1)
	}

	// Commit city transaction
	err = txCity.Commit()
	if err != nil {
		log.Printf("Error committing city transaction: %v", err)
		txCity.Rollback()
		return
	}

	durationCities := time.Since(startTimeCities)

	// Print city summary
	fmt.Printf("\nCity Migration Summary:\n")
	fmt.Printf("=====================\n")
	fmt.Printf("Total cities found: %d\n", totalCities)
	fmt.Printf("Successfully processed cities: %d\n", processedCities)
	fmt.Printf("Successfully inserted cities: %d\n", insertedCities)
	fmt.Printf("Skipped (already exist): %d\n", skippedCities)
	fmt.Printf("Failed transfers: %d\n", errorCities)
	fmt.Printf("\nPerformance Metrics:\n")
	fmt.Printf("Duration: %s\n", durationCities.Round(time.Second))
	fmt.Printf("Average speed: %.2f cities/second\n", float64(processedCities)/durationCities.Seconds())

	// Part 3: Migrate Airports
	fmt.Printf("\nPhase 1.3: Migrating Airports from JSON\n")
	fmt.Printf("====================================\n")

	// Read Airport JSON file
	airportFile, err := os.ReadFile("service/airport/seed/airport/airport-arab.json")
	if err != nil {
		log.Fatal("Error reading airport JSON file:", err)
	}

	var airports []helper.AirportJSON
	err = json.Unmarshal(airportFile, &airports)
	if err != nil {
		log.Fatal("Error parsing airport JSON:", err)
	}

	totalAirports = helper.TotalAirports(airports)
	fmt.Printf("Found %d airports to transfer\n", totalAirports)

	// Prepare airport statements
	getCityIDStmt, err = helper.GetCityIDFromLocationStmt(devIdentityDB)
	if err != nil {
		log.Fatal("Error preparing get city ID statement:", err)
	}
	defer getCityIDStmt.Close()

	checkAirportExistStmt, err = helper.CheckAirportExistStmt(devGeneralDB)
	if err != nil {
		log.Fatal("Error preparing check airport statement:", err)
	}
	defer checkAirportExistStmt.Close()

	insertAirportStmt, err = helper.InsertAirportStmt(devGeneralDB)
	if err != nil {
		log.Fatal("Error preparing insert airport statement:", err)
	}
	defer insertAirportStmt.Close()

	// Progress bar for airports
	airportBar := progressbar.NewOptions(totalAirports,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription("[cyan][3/3][reset] Transferring airports..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))

	var (
		processedAirports int
		insertedAirports  int
		skippedAirports   int
		errorAirports     int
	)

	startTimeAirports := time.Now()

	// Begin airport transaction
	txAirport, err := devGeneralDB.Begin()
	if err != nil {
		log.Fatal("Error starting airport transaction:", err)
	}

	// Process each airport
	for _, airport := range airports {
		newID, err := helper.ProcessAirport(txAirport, airport, getCityIDStmt, checkAirportExistStmt, insertAirportStmt)
		if err != nil {
			log.Printf("Error processing airport %s: %v", airport.Code, err)
			errorAirports++
			airportBar.Add(1)
			continue
		}

		if newID > 0 {
			insertedAirports++
		} else {
			skippedAirports++
		}
		processedAirports++
		airportBar.Add(1)
	}

	// Commit airport transaction
	err = txAirport.Commit()
	if err != nil {
		log.Printf("Error committing airport transaction: %v", err)
		txAirport.Rollback()
		return
	}

	durationAirports := time.Since(startTimeAirports)

	// Print airport summary
	fmt.Printf("\nAirport Migration Summary:\n")
	fmt.Printf("========================\n")
	fmt.Printf("Total airports found: %d\n", totalAirports)
	fmt.Printf("Successfully processed airports: %d\n", processedAirports)
	fmt.Printf("Successfully inserted airports: %d\n", insertedAirports)
	fmt.Printf("Skipped (already exist): %d\n", skippedAirports)
	fmt.Printf("Failed transfers: %d\n", errorAirports)
	fmt.Printf("\nPerformance Metrics:\n")
	fmt.Printf("Duration: %s\n", durationAirports.Round(time.Second))
	fmt.Printf("Average speed: %.2f airports/second\n", float64(processedAirports)/durationAirports.Seconds())
}
