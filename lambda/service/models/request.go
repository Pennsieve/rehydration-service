package models

import "github.com/pennsieve/rehydration-service/shared/models"

type Request struct {
	models.Dataset
	models.User
}
