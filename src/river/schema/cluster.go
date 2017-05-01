package schema

import "time"

type MasterInfo struct {
	Id    string    `json:"id"`
	Since time.Time `json:"since"`
}
