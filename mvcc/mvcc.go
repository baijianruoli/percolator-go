package mvcc



import (
	"percolator_slave1/model"
	"sync"
)


type Mvcc interface {
	MvccGet(key string) *model.Node
	MvccScan(startTs, endTs int64, key,optional string) bool
	MvccPut(key string, version int64, value interface{}) error
	MvccDelete(key string, version int64) error
	MvccDeleteLoc(key string) error
}

type MvccImpl struct {
	Mp map[string][]*model.Node
	rw sync.RWMutex
}

func (m *MvccImpl) MvccGet(key string, version int64) (node *model.Node) {
	m.rw.RLock()
	defer m.rw.RUnlock()

	// 先 通过committs 找到startts
	if v, ok := m.Mp[key]; ok {
		for _, n := range v {
			if n.Version==nil{
				continue
			}
			if n.Version.CommitTs <= version {
				if node == nil {
					node = n
				} else {
					if n.Version.CommitTs > node.Version.CommitTs {
						node = n
					}
				}
			}
		}
	}
	if node==nil{
		return
	}
	// 再通过startts找到 value
	if v, ok := m.Mp[key]; ok {
		for _, n := range v {
			if n.Value==nil{
				continue
			}
			if n.Value.StartTs <= node.Version.StartTs {
				 node=n
			}
		}
	}
	return
}

// 只做新增
func (m *MvccImpl) MvccPut(key string, node *model.Node) error {
	m.rw.Lock()
	defer m.rw.Unlock()
	if _,ok:=m.Mp[key];!ok{
		m.Mp[key]=make([]*model.Node,0)
	}
	m.Mp[key]=append(m.Mp[key],node)
	return nil
}

func (m *MvccImpl) MvccScan(startTs, endTs int64, key,optional string) bool {

		for _,v:=range m.Mp[key]{
			switch optional {
			case "write":
				if v.Version!=nil&&v.Version.CommitTs>=startTs&&v.Version.CommitTs<=endTs{
					 return true
				}
			case "lock":
				if v.Lock!=nil&&v.Lock.StartTs>=startTs&&v.Lock.StartTs<=endTs{
					return true
				}
			}
		}
   return false
}

func (m *MvccImpl) MvccDelete(key string, version int64) error {

		 return nil
}

func (m *MvccImpl) MvccDeleteLock(key string,startTs int64) error{
	m.rw.Lock()
	defer m.rw.Unlock()
	if v,ok:=m.Mp[key];ok{
		 for _,n:=range v{
		 	if n.Lock.StartTs==startTs{
		 		n.Lock=nil
			}
		 }
	}
	return nil
}
