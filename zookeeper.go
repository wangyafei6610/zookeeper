package zookeeper

import (
	"github.com/samuel/go-zookeeper/zk"

	"time"
	"strings"
	"fmt"
	"errors"
)
//全局zk对象
var zkSt *ZKManager
var zkFlags = int32(0) //永久节点
var zkAcl = zk.WorldACL(zk.PermAll)

//zk结构体
type ZKManager struct {
	zkHosts string
	zkTimeout time.Duration
	session *zk.Conn
	eventChan <-chan zk.Event
	isClose bool
	isEffective bool
	watcherPath map[string]string
	watchers map[string]string
	pathData map[string]string
}
//初始化zk结构体对象
func NewZkManager(hosts string, timeout time.Duration) *ZKManager {

	if zkSt != nil && zkSt.isEffective == true{
		return zkSt
	}
	hostsM := strings.Split(hosts,",")
	con,event,err := zk.Connect(hostsM,timeout*time.Second)
	if err != nil {
		panic(fmt.Sprintf("连接zk失败... err:=%v",err))
	}
	zkManagerSt := &ZKManager{}
	zkManagerSt.zkHosts = hosts
	zkManagerSt.zkTimeout = timeout
	zkManagerSt.session = con
	zkManagerSt.eventChan = event
	zkManagerSt.isClose = false
	zkManagerSt.isEffective = true
	zkManagerSt.watcherPath = make(map[string]string,0)
	zkManagerSt.watchers = make(map[string]string,0)
	zkManagerSt.pathData = make(map[string]string,0)
	zkSt = zkManagerSt
	go listenEvent()
	return zkManagerSt
}

//设置监听path
func SetWatcherPaths(paths []string) {

	if len(paths) == 0 {
		panic(fmt.Sprintf("err:=设置监听len(path)=0"))
	}
	for _, path := range paths {
		zkSt.watcherPath[path] = path
		for setWatcherPath(path) == false {
			time.Sleep(1 * time.Second)
		}
	}
}
//执行监听path
func  setWatcherPath(path string) bool {
	child,stat,event,err := zkSt.session.ChildrenW(path)
	fmt.Printf("\nsetWatcherPath|path=%v,child=%+v,stat=%+v,event=%+v,err=%v\n",path,child,stat,event,err)
	if err != nil{
		delete(zkSt.watcherPath,path)
		delete(zkSt.watchers,path)
		//delete(zkSt.pathData,path)
		return false
	}
	bData,stat,event,err := zkSt.session.GetW(path)
	fmt.Printf("%+v-->%+v",path,string(bData))
	if err != nil{
		delete(zkSt.watcherPath,path)
		delete(zkSt.watchers,path)
		//delete(zkSt.pathData,path)
		return false
	}
	zkSt.watcherPath[path] = path
	zkSt.watchers[path] = path
	zkSt.pathData[path] = string(bData)
	getChirldData(path)
	return true
}

//监听变动
func  listenEvent() {
	defer func() {
		if err:= recover(); err != nil {
			fmt.Printf("zk.listenEvent|defer error|err=%v",err)
		}
	}()
	for zkSt.isEffective{
		change := <-zkSt.eventChan
		path := change.Path
		switch change.Type {
		case zk.EventSession:
			if change.State == zk.StateExpired || change.State == zk.StateDisconnected {
				expiredClose()
			}
		case zk.EventNodeCreated, zk.EventNodeDataChanged,zk.EventNodeChildrenChanged:
			fmt.Printf("\nlistenEvent | type=%+v,change=%+v\n",change.Type,change)
			setWatcherPath(path)
		case zk.EventNodeDeleted:
			fmt.Printf("\nlistenEvent | type=%+v,change=%+v\n",change.Type,change)
			if path != "" {
				delete(zkSt.watcherPath,path)
				delete(zkSt.watchers,path)
				delete(zkSt.pathData,path)
			}
			setWatcherPath(path)
			
		}
	}
}
//连接超时或者失效重新创建
func expiredClose()  {
	zkSt.isClose = true
	zkSt.isEffective = false
	var paths = make([]string,0)
	for _,key:= range zkSt.watcherPath  {
		paths = append(paths,key)
	}
	NewZkManager(zkSt.zkHosts,zkSt.zkTimeout)
	SetWatcherPaths(paths)

}
//节点变动
func nodeChange(path string)  {
	setWatcherPath(path)
	getChirldData(path)
}
//获取节点下所有元素的值
func getChirldData(path string)  {
	children,_,_,err := zkSt.session.ChildrenW(path)
	if err != nil {

	}
	if len(children) > 0 {
		for _,child := range children {
			key := ""
			if path == "/" {
				key = fmt.Sprintf("%v%v",path,child)
			}else{
				key = fmt.Sprintf("%v/%v",path,child)
			}

			childData,_,_,err := zkSt.session.GetW(key)
			if err != nil {

			}
			zkSt.pathData[key] = string(childData)
		}
	}
}

//查询数据
func GetNode(path string) string {
	return zkSt.pathData[path]
}
func  GetPathData() map[string]string {
	return zkSt.pathData
}

func CreateNode(path,value string) (string,error) {
	if !checkSessionEffective() {
		return "",errors.New("zk连接已经失去连接")
	}
	str,err := zkSt.session.Create(path,[]byte(value),zkFlags,zkAcl)
	return str,err
}

func NodeExist(path string) (bool,error) {
	if !checkSessionEffective() {
		return false,errors.New("zk连接已经失去连接")
	}
	boo,_,err := zkSt.session.Exists(path)
	return boo,err
}

func SetNode(path,value string) (bool,error) {
	if !checkSessionEffective() {
		return false,errors.New("zk连接已经失去连接")
	}
	_,stat,err := zkSt.session.Get(path)
	if err != nil{
		return false,err
	}
	stat,err = zkSt.session.Set(path,[]byte(value),stat.Version)
	if err != nil{
		return false,err
	}
	return true ,err
}

func  GetZKNode(path string) (string,error) {
	if !checkSessionEffective() {
		return "",errors.New("zk连接已经失去连接")
	}
	bData,_,err := zkSt.session.Get(path)
	return string(bData),err
}
func  DeleteNode(path string) (bool,error)  {
	if !checkSessionEffective() {
		return false,errors.New("zk连接已经失去连接")
	}
	_,_,err := zkSt.session.Get(path)
	return true, err
}
func checkSessionEffective() bool {
	if zkSt.isEffective == false{
		zkSt.isClose = false
		expiredClose()
	}
	return zkSt.isEffective
}