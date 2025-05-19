package services

import (
	"op-log-parser/application/domain/models"
)

type ParserService interface {
	Parse(oplogJSON string) ([]string, error)

	ProcessOpLog(opLog models.OpLog) ([]string, error)
}

type SchemaTracker interface {
	IsDDLGenerated(namespace string) bool

	MarkDDLGenerated(namespace string)

	GetKnownColumns(namespace string) map[string]bool

	InitializeColumnTracker(namespace string, data map[string]any)

	UpdateColumnsTracker(namespace string, newFields map[string]any)
}

type UUIDGenerator func() string
