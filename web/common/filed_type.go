package common

import (
	"equinox/storage/types"
	"gorm.io/gorm"
)

type FieldTypeEntry struct {
	gorm.Model
	Key       string          `gorm:"type:varchar(255);index"`
	Field     string          `gorm:"type:varchar(255);index"`
	FieldType types.FieldType `gorm:"type:integer"`
}
