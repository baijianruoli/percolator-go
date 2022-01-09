package mvcc

import "percolator_slave1/model"

type Operation struct {
	Type  int
	Node  *model.Node
	Mvcc  *MvccImpl
}

const (
	PUT = iota
	DELETE
)