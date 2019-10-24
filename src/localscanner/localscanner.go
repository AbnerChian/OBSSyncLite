package localscanner

import (
	"../logmgr"
	"fmt"
	"github.com/sirupsen/logrus"
	"strings"

	//"github.com/fsnotify"
	"github.com/radovskyb/watcher"
	"io/ioutil"
	"os"
	"sync"
	"time"
)

//var logger = logrus.New()
var log *logrus.Entry

//var eventsChan chan *FSEvent
var dirSyncStats sync.Map

func init() {
	log = logmgr.NewLogger()
}

type LocalScanner struct {
	ScanPaths     []string
	IgnoreSufixes []string
	QueueSize     int64
	EventsChan    chan *FSEvent
	DirStatMap    sync.Map
	FSWatcher     *watcher.Watcher
}

/*
文件系统监听，并将监听到的文件系统事件加入到同步队列
*/
func (q *LocalScanner) DoDetecting() {
	defer q.FSWatcher.Close()

	for {
		select {
		// 监听文件系统事件
		case event := <-q.FSWatcher.Event:
			log.Infof("FS_EVENT:%s, OP:%v\n", event.Path, event.Op)
			q.EventsChan <- &FSEvent{event, time.Now()}
		case error := <-q.FSWatcher.Error:
			log.Warningf("Received an error from FS watcher! error:[%v]", error)
			//待确定一下watcher出现error后的其他表现，怎么recover
		}
	}
}

/*
将目录以及目录下的所有子目录添加到监听中，同时将当前目录下的文件全部加到文件同步队列
*/
func (q *LocalScanner) FullDirScan(scanPath string, dataSyncChan chan<- *DataSyncEvent) {
	f, err := os.Stat(scanPath)
	//目录存在或者是文件，不用添加扫描
	if err != nil || !f.IsDir() {
		//文件、目录不存在，退出
		log.Debugf("Full dir scan, file/dir does not exist. path:[%s]", scanPath)
		return
	}

	//列举目录下的文件和子目录
	subDirs, err := ioutil.ReadDir(scanPath)
	//目录列举出错即退出
	if err != nil {
		log.Warningf("Failed to list files and sub directories under [%s], Fully scanning exit!\n", scanPath)
		return
	}

	for _, f := range subDirs {
		if f.IsDir() {
			//递归FullDirScan子目录
			go q.FullDirScan(fmt.Sprintf("%s%s%s", scanPath, string(os.PathSeparator), f.Name()), dataSyncChan)
			continue
		}
		//目录下的文件直接加入同步队列
		log.Debugf("List and find a file [%s%s%s]", scanPath, string(os.PathSeparator), f.Name())
		dataSyncChan <- &DataSyncEvent{PathName: fmt.Sprintf("%s%s%s", scanPath, string(os.PathSeparator), f.Name()), IsDir: false, Operation: FileSync, EventTime: time.Now()}
	}
}

//从文件系统时间队列分拣时间，将文件、目录的各类FS的Operation转义成数据同步时间的文件上传、文件删除、整目录删除3种事件
func (q *LocalScanner) EventHandler(dataSyncChan chan<- *DataSyncEvent) {
	for {
		fsevent := <-q.EventsChan
		log.Debugf("EventHandler: Event Name:[%s], Event Op:[%v], IsFile:[%v]", fsevent.Path, fsevent.Op, fsevent.IsDir())
		switch fsevent.Op {
		case watcher.Create:
			switch fsevent.IsDir() {
			//创建文件
			case false:
				dataSyncChan <- &DataSyncEvent{fsevent.Path, false, FileSync, fsevent.ModTime()}
				//新创建空目录，重命名目录，移动目录，拷贝目录
			// 创建文件夹
			case true:
				continue
				//dataSyncChan <- &DataSyncEvent{fsevent.Path, true, FileSync, fsevent.EventTime}
			default:
				log.Warningf("Can not find the newly created file or directory! [%s]", fsevent.Name)
			}
		case watcher.Write:
			switch fsevent.IsDir() {
			case false:
				dataSyncChan <- &DataSyncEvent{fsevent.Path, false, FileSync, fsevent.ModTime()}
				//如果是目录或者其他场景，Write的事件不处理
			default:
				continue
			}
		case watcher.Remove:
			switch fsevent.IsDir() {
			case false:
				dataSyncChan <- &DataSyncEvent{fsevent.Path, false, FileRemove, fsevent.ModTime()}
			case true:
				dataSyncChan <- &DataSyncEvent{fsevent.Path, true, DirRemove, fsevent.ModTime()}
			}
		case watcher.Rename:
			switch fsevent.IsDir() {
			case true:
				paths := strings.Split(fsevent.Path, "->")
				if len(paths) < 2 {
					log.Warningf("Invalid event path [%s] with operation [%s].", fsevent.Path, fsevent.Op)
					continue
				}
				dataSyncChan <- &DataSyncEvent{strings.Trim(paths[0], " "), true, DirRemove, fsevent.ModTime()}
			case false:
				paths := strings.Split(fsevent.Path, "->")
				if len(paths) < 2 {
					log.Warningf("Invalid event path [%s] with operation [%s].", fsevent.Path, fsevent.Op)
					continue
				}
				log.Debugf("Move operation, move file, source file:[%s], target file:[%s]", paths[0], paths[1])
				dataSyncChan <- &DataSyncEvent{strings.Trim(paths[0], " "), false, FileRemove, fsevent.ModTime()}
				dataSyncChan <- &DataSyncEvent{strings.Trim(paths[1], " "), false, FileSync, fsevent.ModTime()}
			}
		case watcher.Move:
			switch fsevent.IsDir() {
			case true:
				paths := strings.Split(fsevent.Path, "->")
				if len(paths) < 2 {
					log.Warningf("Invalid event path [%s] with operation [%s].", fsevent.Path, fsevent.Op)
					continue
				}
				dataSyncChan <- &DataSyncEvent{strings.Trim(paths[0], " "), true, DirRemove, fsevent.ModTime()}
			case false:
				paths := strings.Split(fsevent.Path, "->")
				if len(paths) < 2 {
					log.Warningf("Invalid event path [%s] with operation [%s].", fsevent.Path, fsevent.Op)
					continue
				}
				log.Debugf("Move operation, move file, source file:[%s], target file:[%s]", paths[0], paths[1])
				dataSyncChan <- &DataSyncEvent{strings.Trim(paths[0], " "), false, FileRemove, fsevent.ModTime()}
				dataSyncChan <- &DataSyncEvent{strings.Trim(paths[1], " "), false, FileSync, fsevent.ModTime()}
			}
		default:
			continue

		}
	}
}
