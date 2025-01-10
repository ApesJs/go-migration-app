package helper

import (
	"database/sql"
	"fmt"
)

func EnsureWukalaRole(db *sql.DB) error {
	var roleExists bool
	err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM "role" WHERE slug = $1)`, "wukala").Scan(&roleExists)
	if err != nil {
		return fmt.Errorf("error checking wukala role: %v", err)
	}

	if !roleExists {
		_, err = db.Exec(`INSERT INTO "role" (name, slug) VALUES ($1, $2)`, "Wukala", "wukala")
		if err != nil {
			return fmt.Errorf("error inserting wukala role: %v", err)
		}
		fmt.Println("Created 'wukala' role")
	}
	return nil
}

func CountTotalRecords(db *sql.DB) (totalRows int, totalTravelAgents int, err error) {
	err = db.QueryRow("SELECT COUNT(*) FROM td_user WHERE role = 'user' AND soft_delete = 'false'").Scan(&totalRows)
	if err != nil {
		return 0, 0, fmt.Errorf("error counting rows: %v", err)
	}

	err = db.QueryRow(`
		SELECT COUNT(*) 
		FROM td_user u
		JOIN td_travel_agent t ON u.id = t.user_id
		WHERE u.role = 'user' AND u.soft_delete = 'false'
	`).Scan(&totalTravelAgents)
	if err != nil {
		return 0, 0, fmt.Errorf("error counting travel agents: %v", err)
	}

	return totalRows, totalTravelAgents, nil
}
