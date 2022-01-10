package coordinator

import (
	"errors"
	"log"
	"percolator_slave1/model"
	"percolator_slave1/mvcc"
	"sync"
	"time"
)

const ENDTS = 1e17

type Transaction struct {
	Flag      bool
	Lock      sync.Mutex
	Operation []*mvcc.Operation
}

func (t *Transaction) Begin() {
	t.Lock.Lock()
	defer t.Lock.Unlock()
	t.Flag = true
}

func (t *Transaction) Prewrite(node, primary *mvcc.Operation, startTs int64) error {
	if node.Mvcc.MvccScan(startTs, ENDTS, node.Node.Key, "write") {
		return errors.New("conflict")
	}
	if node.Mvcc.MvccScan(0, ENDTS, node.Node.Key, "lock") {
		return errors.New("conflict")
	}
	err := node.Mvcc.MvccPut(node.Node.Key, &model.Node{
		Key: node.Node.Key,
		Value: &model.Value{
			StartTs: startTs,
			Value:   node.Node.Value.Value,
		},
	})
	if err != nil {
		log.Fatal("put error")
		return errors.New("put error")
	}
	err = node.Mvcc.MvccPut(node.Node.Key, &model.Node{
		Key: node.Node.Key,
		Lock: &model.Lock{
			StartTs:    startTs,
			PrimaryRow: primary.Node.Key,
		},
	})
	if err != nil {
		log.Fatal("put error")
		return errors.New("put error")
	}
	return nil
}

func (t *Transaction) Write(op *mvcc.Operation, ts, commitTs int64) error {
	if !op.Mvcc.MvccScan(ts, ts, op.Node.Key, "lock") {
		return errors.New("conflict")
	}
	err := op.Mvcc.MvccPut(op.Node.Key, &model.Node{
		Key: op.Node.Key,
		Version: &model.Write{
			StartTs:  ts,
			CommitTs: commitTs,
		},
	})
	if err != nil {
		log.Fatal("put error")
		return errors.New("put error")
	}
	err = op.Mvcc.MvccDelete(op.Node.Key, ts)
	if err != nil {
		log.Fatal("MvccDelete error")
		return errors.New("MvccDelete error")
	}
	return nil
}

func (t *Transaction) Commit() error {
	t.Lock.Lock()
	defer t.Lock.Unlock()
	ts := time.Now().Unix()
	// primaryRow写入
	var primary *mvcc.Operation
	if len(t.Operation) > 0 {
		primary = t.Operation[0]
		if err := t.Prewrite(primary, primary, ts); err != nil {
			return err
		}
	}
	//  secondary写入
	t.Operation = t.Operation[1:]
	for _, v := range t.Operation {
		if err := t.Prewrite(v, primary, ts); err != nil {
			return err
		}
	}
	// 开始commit
	commitTs := time.Now().Unix()
	if primary != nil {
		if err := t.Write(primary, ts, commitTs); err != nil {
			return err
		}
	}
	// 异步执行剩下的节点
	for _, v := range t.Operation {
		go t.Write(v, ts, commitTs)
	}
	return nil
}
