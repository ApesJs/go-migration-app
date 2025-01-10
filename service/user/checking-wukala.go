package user

import (
	"fmt"
	"github.com/ApesJs/go-migration-app/database"
	"log"
	"time"
)

func CheckingWukalaService() {
	// Membuat koneksi database
	prodExistingUmrahDB := database.ConnectionProdExistingUmrahDB()
	defer prodExistingUmrahDB.Close()

	//DevIdentityDB := database.ConnectionDevIdentityDB()
	//defer DevIdentityDB.Close()

	localIdentityDB := database.ConnectionLocalIdentityDB()
	defer localIdentityDB.Close()

	waktuMulai := time.Now()
	fmt.Println("Memulai proses pengecekan Wukala...")

	// Mengambil semua user_id dari td_travel_agent (sumber)
	var sourceUserIDs []string
	sourceRows, err := prodExistingUmrahDB.Query("SELECT user_id FROM td_travel_agent")
	if err != nil {
		log.Fatal("Error saat mengambil data dari database sumber:", err)
	}
	defer sourceRows.Close()

	for sourceRows.Next() {
		var userID string
		if err := sourceRows.Scan(&userID); err != nil {
			log.Fatal("Error saat membaca baris data sumber:", err)
		}
		sourceUserIDs = append(sourceUserIDs, userID)
	}

	// Mengambil semua id dari tabel user dengan role wukala (target)
	var targetUserIDs []string
	targetRows, err := localIdentityDB.Query(`SELECT id FROM "user" WHERE role = 'wukala'`)
	if err != nil {
		log.Fatal("Error saat mengambil data dari database target:", err)
	}
	defer targetRows.Close()

	for targetRows.Next() {
		var userID string
		if err := targetRows.Scan(&userID); err != nil {
			log.Fatal("Error saat membaca baris data target:", err)
		}
		targetUserIDs = append(targetUserIDs, userID)
	}

	// Membuat map untuk pencarian yang lebih cepat
	sourceMap := make(map[string]bool)
	targetMap := make(map[string]bool)

	for _, id := range sourceUserIDs {
		sourceMap[id] = true
	}
	for _, id := range targetUserIDs {
		targetMap[id] = true
	}

	// Mencari ID yang ada di target tapi tidak ada di sumber
	var wukalaTambahan []string
	for _, id := range targetUserIDs {
		if !sourceMap[id] {
			wukalaTambahan = append(wukalaTambahan, id)
		}
	}

	// Mencari ID yang ada di sumber tapi tidak ada di target
	var wukalaTidakAda []string
	for _, id := range sourceUserIDs {
		if !targetMap[id] {
			wukalaTidakAda = append(wukalaTidakAda, id)
		}
	}

	// Menghitung ID yang cocok
	var jumlahCocok int
	for _, id := range sourceUserIDs {
		if targetMap[id] {
			jumlahCocok++
		}
	}

	// Menampilkan hasil
	durasi := time.Since(waktuMulai)
	fmt.Printf("\nHasil Pengecekan Wukala:\n")
	fmt.Printf("------------------------\n")
	fmt.Printf("Total travel agent di database sumber: %d\n", len(sourceUserIDs))
	fmt.Printf("Total user wukala di database target: %d\n", len(targetUserIDs))
	fmt.Printf("Jumlah ID yang cocok: %d\n", jumlahCocok)

	if len(wukalaTambahan) > 0 {
		fmt.Printf("\nDaftar Wukala Tambahan (ada di target tapi tidak ada di sumber):\n")
		fmt.Printf("--------------------------------------------------------\n")
		for i, id := range wukalaTambahan {
			// Mengambil detail user untuk wukala tambahan
			var nama, email string
			err := localIdentityDB.QueryRow(`SELECT name, email FROM "user" WHERE id = $1`, id).Scan(&nama, &email)
			if err != nil {
				log.Printf("Error saat mengambil detail untuk user %s: %v", id, err)
				continue
			}
			fmt.Printf("%d. ID: %s (Nama: %s, Email: %s)\n", i+1, id, nama, email)
		}
		fmt.Printf("\nTotal wukala tambahan: %d\n", len(wukalaTambahan))
	}

	if len(wukalaTidakAda) > 0 {
		fmt.Printf("\nDaftar Wukala Yang Tidak Ada (ada di sumber tapi tidak ada di target):\n")
		fmt.Printf("------------------------------------------------------------\n")
		for i, id := range wukalaTidakAda {
			// Mengambil detail user untuk wukala yang tidak ada
			var nama, email string
			err := prodExistingUmrahDB.QueryRow("SELECT name, email FROM td_user WHERE id = $1", id).Scan(&nama, &email)
			if err != nil {
				log.Printf("Error saat mengambil detail untuk user %s: %v", id, err)
				continue
			}
			fmt.Printf("%d. ID: %s (Nama: %s, Email: %s)\n", i+1, id, nama, email)
		}
		fmt.Printf("\nTotal wukala yang tidak ada: %d\n", len(wukalaTidakAda))
	}

	fmt.Printf("\nProses selesai dalam waktu: %s\n", durasi.Round(time.Millisecond))
}
