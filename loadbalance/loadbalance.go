package loadbalance

import (
	"errors"
	"percolator_slave1/coordinator"
	"percolator_slave1/model"
	"percolator_slave1/mvcc"
	"time"
)

type LoadBalance struct {
	Clients []*mvcc.MvccImpl
	Tx      *coordinator.Transaction
}

func (m *LoadBalance) MvccGet(key string) (*model.Node, error) {
	ts := time.Now().Unix()
	client := m.Clients[hash(key)%len(m.Clients)]
	if client.MvccScan(0, ts-1, key, "lock") {
		// 尝试锁消除
		if client.ReMvccScan(0, ts-1, key) {
			return nil, errors.New("conflict")
		}
	}
	node := client.MvccGet(key, ts)
	return node, nil
}

func (m *LoadBalance) MvccPut(key string, value interface{}) error {
	client := m.Clients[hash(key)%len(m.Clients)]
	m.Tx.Operation = append(m.Tx.Operation, &mvcc.Operation{
		Mvcc: client,
		Type: mvcc.PUT,
		Node: &model.Node{
			Key: key,
			Value: &model.Value{
				Value: value,
			},
		},
	})
	return nil
}

// 对外不提供scan操作
func (m *LoadBalance) MvccScan(startKey, endKey string, version, limit int64) []*model.Node {
	return nil
}

// 对外不提供delete操作
func (m *LoadBalance) MvccDelete(key string, version int64) error {
	return nil
}

// 随便写的hash函数
func hash(key string) int {
	var md int = 1e9 + 7
	res := 1
	for _, v := range key {
		res = (res*10 + int(v)) % md
	}
	return res
}
