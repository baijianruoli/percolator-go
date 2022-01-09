package main

import (
	"fmt"
	"log"
	"percolator_slave1/coordinator"
	"percolator_slave1/loadbalance"
	"percolator_slave1/model"
	"percolator_slave1/mvcc"
)

func main() {
	m1:=&mvcc.MvccImpl{Mp: make(map[string][]*model.Node)}
	m2:=&mvcc.MvccImpl{Mp: make(map[string][]*model.Node)}
	m3:=&mvcc.MvccImpl{Mp: make(map[string][]*model.Node)}
	tx:=&coordinator.Transaction{Operation: make([]*mvcc.Operation,0)}
	ld:=&loadbalance.LoadBalance{Clients: []*mvcc.MvccImpl{m1,m2,m3},Tx: tx}
	tx.Begin()
     ld.MvccPut("1","1")
     ld.MvccPut("2","2")
     ld.MvccPut("3","3")
	if err:=tx.Commit();err!=nil{
		log.Println(err.Error())
	}
	res,_:=ld.MvccGet("2")
	fmt.Println(res.Value.Value)
	fmt.Printf("%+v\n",m1.Mp)
	fmt.Printf("%+v\n",m2.Mp)
	fmt.Printf("%+v\n",m3.Mp)

}
