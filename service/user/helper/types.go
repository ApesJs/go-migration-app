package helper

import (
	"database/sql"
)

type Statements struct {
	CheckTravelAgent *sql.Stmt
	Check            *sql.Stmt
	Insert           *sql.Stmt
}

type TxStatements struct {
	Check  *sql.Stmt
	Insert *sql.Stmt
}

type TransferStats struct {
	TransferredCount   int
	ErrorCount         int
	SkipCount          int
	WukalaCount        int
	DuplicateEmails    []string
	SkippedTravelAgent map[string]string
}

func (s *Statements) CloseAll() {
	if s.CheckTravelAgent != nil {
		s.CheckTravelAgent.Close()
	}
	if s.Check != nil {
		s.Check.Close()
	}
	if s.Insert != nil {
		s.Insert.Close()
	}
}
