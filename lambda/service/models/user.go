package models

type User struct {
	ID     int64
	NodeID string
	Name   string `json:"name"`
	Email  string `json:"email"`
}
