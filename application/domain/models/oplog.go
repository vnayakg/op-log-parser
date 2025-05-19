package models

type OpLog struct {
	Operation string         `bson:"op" json:"op"`
	Namespace string         `bson:"ns" json:"ns"`
	Data      map[string]any `bson:"o" json:"o"`
	O2        *O2Field       `bson:"o2,omitempty" json:"o2,omitempty"`
}

type O2Field struct {
	ID string `bson:"_id" json:"_id"`
}

const (
	Insert = "i"
	Update = "u"
	Delete = "d"
)

const (
	FieldID    = "_id"
	FieldDiff  = "diff"
	FieldSet   = "u"
	FieldUnset = "d"
	FieldNull  = "NULL"
)
