package datacheck

import (
	"../localscanner"
	"../logmgr"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"obs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type CheckType int

const (
	FullCheck   CheckType = iota //输出本地
	LocalFirst                   //根据本地文件校验远端文件情况，把本地没有同步到远端的文件补充同步到远端
	RemoteFirst                  //根据远端文件校验本地文件
)

const (
	HTTP404 int = 404
	HTTP200 int = 200
)

var log *logrus.Entry

func init() {
	log = logmgr.NewLogger()
}

type DataCheck struct {
	CheckInterval    time.Duration //校验周期，建议以小时为单位
	CheckMethod      CheckType     //校验方法
	IsCheckMD5       bool          //是否校验MD5
	LocalDirs        []string      //本地校验的路径
	RemotePrefix     string        //远端桶内文件前缀
	Bucket           string
	EndPoint         string
	AK               string
	SK               string
	CheckConcurrency int //校验并发度
	DataSyncChan     chan<- *localscanner.DataSyncEvent
	CheckEventChan   chan *CheckEvent
	walkerChan       chan int
}

type CheckEvent struct {
	LocalPath   string
	CheckMethod CheckType
	IsCheckMD5  bool
	EventTime   time.Time
}

func (q *DataCheck) StartCheck() {
	//启动比对协程
	q.walkerChan = make(chan int, q.CheckConcurrency)
	go q.startCheckRoutinesPool()
	//触发比对
	for {
		log.Infof("Start to check local data!")
		q.localFirstCheck()
		time.Sleep(q.CheckInterval)
	}
}

func (q *DataCheck) localFirstCheck() {
	for _, dir := range q.LocalDirs {
		err := filepath.Walk(dir, q.checkWalk)
		if err != nil {
			log.Errorf("Failed to walk on dir [%s] to make data checking. Error:[%s]", dir, err)
		}
	}
}

func (q *DataCheck) checkWalk(path string, f os.FileInfo, err error) error {
	//文件夹不做处理
	if f.IsDir() {
		return nil
	}
	q.CheckEventChan <- &CheckEvent{LocalPath: path, CheckMethod: q.CheckMethod, IsCheckMD5: q.IsCheckMD5, EventTime: time.Now()}
	return nil
}

func (q *DataCheck) startCheckRoutinesPool() {
	for i := 0; i < q.CheckConcurrency; i++ {
		go q.checkWalker()
	}
	for {
		<-q.walkerChan
		go q.checkWalker()
	}

}

func (q *DataCheck) checkWalker() {
	defer q.enableRoutine()
	client, err := q.initObsClient()
	if err != nil {
		log.Fatalf("Failed to initialize ")
	}
	for event := range q.CheckEventChan {
		getObjectMeta := &obs.GetObjectMetadataInput{}
		getObjectMeta.Bucket = q.Bucket
		getObjectMeta.Key = fmt.Sprintf("%s%s", filepath.ToSlash(q.RemotePrefix), filepath.ToSlash(event.LocalPath))
		res, err := client.GetObjectMetadata(getObjectMeta)
		if err != nil {
			if strings.Contains(err.Error(), "Status=404 Not Found") {
				log.Debugf("DataCheck find a local file which does not exist on the remote. File:[%s]", event.LocalPath)
				q.DataSyncChan <- &localscanner.DataSyncEvent{PathName: event.LocalPath, IsDir: false, Operation: localscanner.FileSync, EventTime: event.EventTime}
			} else {
				log.Warningf("Data check failed while try to head remote object, due to [%s], check file:[%s]", err, event.LocalPath)
			}
			continue
		}
		//远端对象不存在，启动同步
		switch res.StatusCode {
		case HTTP404:
			q.DataSyncChan <- &localscanner.DataSyncEvent{PathName: event.LocalPath, IsDir: false, Operation: localscanner.FileSync, EventTime: event.EventTime}
		case HTTP200:
			continue
			// 待细化比对MD5场景
		}
	}
}

func (q *DataCheck) initObsClient() (*obs.ObsClient, error) {
	client, err := obs.New(q.AK, q.SK, q.EndPoint)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func calcMD5(filePath string, partSize int64) string {
	fd, err := os.Stat(filePath)
	if err != nil {
		log.Warningf("Failed to get file information before calculate content md5 of file [%s].", filePath)
		return ""
	}
	if fd.Size() <= partSize {
		h := md5.New()
		f, err := os.Open(filePath)
		if err != nil {
			log.Warningf("Failed to calculate content md5 of file [%s].", filePath)
			return ""
		}
		defer f.Close()
		io.Copy(h, f)
		return hex.EncodeToString(h.Sum(nil))
	} else {
		return ""
	}
}

func (q *DataCheck) enableRoutine() {
	q.walkerChan <- 0
}
