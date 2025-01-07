package helper

import (
	"database/sql"
)

func PrepareStatements(sourceDB, targetDB *sql.DB) (*Statements, error) {
	checkTravelAgent, err := sourceDB.Prepare(`
		SELECT EXISTS(
			SELECT 1 
			FROM td_travel_agent t
			JOIN td_user u ON t.user_id = u.id
			WHERE t.user_id = $1 
			AND u.role = 'user' 
			AND u.soft_delete = 'false'
			AND t.code IS NOT NULL
    	)
	`)
	if err != nil {
		return nil, err
	}

	check, err := targetDB.Prepare(`SELECT COUNT(*) FROM "user" WHERE email = $1`)
	if err != nil {
		return nil, err
	}

	insert, err := targetDB.Prepare(`
		INSERT INTO "user" (
			id, name, username, email, role,
		    is_active, email_verified,
			avatar, avatar_provider, provider,
			deleted, created_at, modified_at,
			created_by, modified_by
		) VALUES (
			$1, $2, $3, $4, $5,
			true, false,
			null, $6, null,
			$7, $8, $9,
			$10, null
		)
	`)
	if err != nil {
		return nil, err
	}

	return &Statements{
		CheckTravelAgent: checkTravelAgent,
		Check:            check,
		Insert:           insert,
	}, nil
}

func CreateTxStatements(tx *sql.Tx, stmts *Statements) *TxStatements {
	return &TxStatements{
		Check:  tx.Stmt(stmts.Check),
		Insert: tx.Stmt(stmts.Insert),
	}
}
