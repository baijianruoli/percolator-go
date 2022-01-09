package model

type Node struct {
	Key       string
	Value     *Value
	Version   *Write
	Lock      *Lock
	ExtraInfo interface{}
}

type Write struct {
	StartTs int64
	CommitTs int64
}

type Lock struct {
	StartTs int64
	PrimaryRow string
}

type Value struct {
	StartTs int64
	Value interface{}
}
