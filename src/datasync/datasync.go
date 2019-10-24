package datasync

import (
	"../localscanner"
	"../logmgr"
	"fmt"
	"github.com/sirupsen/logrus"
	"obs"
	"os"
	"path/filepath"
	"time"
)

var log *logrus.Entry

const (
	MB      int64  = 1024 * 1024
	EVENTNS string = "x-obs-meta-event-ns"
)

//初始化，demo，待完善
func init() {
	log = logmgr.NewLogger()
}

type DataSync struct {
	EndPoint         string
	AK               string
	SK               string
	IsHTTPS          bool
	IsLongConnection bool
	Bucket           string
	Prefix           string
	Concurrency      int
	PartSize         int64

	DataSyncChan   chan *localscanner.DataSyncEvent
	goRoutinesChan chan int
}

func (q *DataSync) StartSync() {
	q.goRoutinesChan = make(chan int, q.Concurrency)
	for i := 0; i < q.Concurrency; i++ {
		go q.SyncWorker()
	}
	for {
		//channel里有value，说明有goroutine 退出了，重新创建一个协程，没有value的时候阻塞，无限等待
		<-q.goRoutinesChan
		go q.SyncWorker()
	}
}

func (q *DataSync) SyncWorker() {
	defer q.enableRoutine()
	obsClient, err := q.initObsClient()
	if err != nil {
		log.Errorf("Failed to initialize client due to [%s].", err)
		return
	}
	for {
		event := <-q.DataSyncChan
		//单个文件上传
		switch event.Operation {
		case localscanner.FileSync:
			log.Debugf("DataSync Sync Single File:[%s]", event.PathName)
			q.syncSingleFile(obsClient, event)
		case localscanner.FileRemove:
			log.Debugf("DataSync Remove Single File:[%s]", event.PathName)
			q.removeSingleFile(obsClient, event)
		case localscanner.DirRemove:
			log.Debugf("DataSync Remove Dir:[%s]", event.PathName)
			q.removeDir(obsClient, event)
		default:
			log.Errorf("Received an invalid event, can not sync!")
		}
	}
}

func (q *DataSync) initObsClient() (*obs.ObsClient, error) {
	client, err := obs.New(q.AK, q.SK, q.EndPoint)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func (q *DataSync) syncSingleFile(obsClient *obs.ObsClient, event *localscanner.DataSyncEvent) {
	f, err := os.Stat(event.PathName)
	if err != nil {
		log.Warningf("Sync file [%s] failed due to [%v]", event.PathName, err)
		return
	}
	//创建以/结尾的对象，表示文件夹
	if event.IsDir {
		putObjRequest := new(obs.PutFileInput)
		putObjRequest.Bucket = q.Bucket
		putObjRequest.Key = fmt.Sprintf("%s%s%s", filepath.ToSlash(q.Prefix), filepath.ToSlash(event.PathName), "/")
		//putObjRequest.Metadata = make(map[string]string)
		//putObjRequest.Metadata["x-obs-meta-event-ns"] = string(event.EventTime.UnixNano())
		putObjRequest.ContentLength = 0
		putObjRequest.SourceFile = event.PathName
		result, err := obsClient.PutFile(putObjRequest)
		if err != nil {
			log.Warningf("Sync file [%s] failed. Due to [%s]", event.PathName, err)
			return
		}
		if result.StatusCode != 200 {
			log.Warningf("Sync fill [%s] failed. Status Code [%d], RequestID [%s]", event.PathName, result.StatusCode, result.RequestId)
		} else {
			log.Infof("Sync file [%s] successfully!", event.PathName)
		}
		return
	}

	//文件大小小于单段大小，上传整个文件
	if f.Size() <= int64(q.PartSize)*MB {
		f, err := os.Stat(event.PathName)
		if err != nil {
			log.Warningf("Can not find file [%s] while trying to sync it!", event.PathName)
			return
		}
		putObjRequest := new(obs.PutFileInput)
		putObjRequest.Bucket = q.Bucket
		putObjRequest.Key = fmt.Sprintf("%s%s", filepath.ToSlash(q.Prefix), filepath.ToSlash(event.PathName))
		//putObjRequest.Metadata = make(map[string]string)
		//putObjRequest.Metadata["x-obs-meta-event-ns"] = string(event.EventTime.UnixNano())
		putObjRequest.ContentLength = f.Size()
		putObjRequest.SourceFile = event.PathName
		result, err := obsClient.PutFile(putObjRequest)
		if err != nil {
			log.Warningf("Sync file [%s] failed. Due to [%s]", event.PathName, err)
			return
		}
		if result.StatusCode != 200 {
			log.Warningf("Sync fill [%s] failed. Status Code [%d], RequestID [%s]", event.PathName, result.StatusCode, result.RequestId)
		} else {
			log.Infof("Sync file [%s] successfully!", event.PathName)
		}

	} else { //文件大小超过单段大小，多段上传
		initMultUpload := new(obs.InitiateMultipartUploadInput)
		initMultUpload.Bucket = q.Bucket
		initMultUpload.Key = fmt.Sprintf("%s%s", filepath.ToSlash(q.Prefix), filepath.ToSlash(event.PathName))
		//initMultUpload.Metadata = make(map[string]string)
		log.Debugf("Event UnixNano:[%d] of file [%s]", event.EventTime.UnixNano(), event.PathName)
		//initMultUpload.Metadata[EVENTNS] = strconv.A
		initResult, err := obsClient.InitiateMultipartUpload(initMultUpload)
		if err != nil {
			log.Warningf("Sync file [%s] failed. Due to [%s]", event.PathName, err)
			return
		}
		if initResult.StatusCode != 200 { //初始化多段失败
			log.Warningf("Sync fill [%s] failed. Operation:Initialize multi parts. Status Code [%d], RequestID [%s]", event.PathName, initResult.StatusCode, initResult.RequestId)
			return
		}
		log.Debugf("Initialize multipart task successfully. UploadID:[%s], file:[%s]", initResult.UploadId, event.PathName)

		var partNum int = 1
		completeParts := &obs.CompleteMultipartUploadInput{}
		completeParts.Bucket = q.Bucket
		completeParts.Key = fmt.Sprintf("%s%s", filepath.ToSlash(q.Prefix), filepath.ToSlash(event.PathName))
		completeParts.UploadId = initResult.UploadId
		completeParts.Parts = make([]obs.Part, 0, 1)

		for ; int64(partNum) <= int64(f.Size()/(int64(q.PartSize)*MB))+1; partNum++ {
			uploadPartInput := new(obs.UploadPartInput)
			uploadPartInput.Bucket = q.Bucket
			uploadPartInput.Key = fmt.Sprintf("%s%s", filepath.ToSlash(q.Prefix), filepath.ToSlash(event.PathName))
			uploadPartInput.SourceFile = event.PathName
			uploadPartInput.PartNumber = partNum
			uploadPartInput.Offset = int64(partNum-1) * q.PartSize * MB
			if (f.Size() - int64(partNum-1)*q.PartSize*MB) > q.PartSize*MB {
				uploadPartInput.PartSize = q.PartSize * MB
			} else {
				uploadPartInput.PartSize = f.Size() - int64(partNum-1)*q.PartSize*MB
			}
			uploadPartInput.UploadId = initResult.UploadId
			partResult, err := obsClient.UploadPart(uploadPartInput)
			if err != nil {
				log.Warningf("Upload part [%d] failed for file [%s], UploadID:[%s], File name:[%s]. Due to [%s]", partNum, event.PathName, initResult.UploadId, event.PathName, err)
				log.Warningf("Sync file [%s] failed. Due to [%s]", event.PathName, err)
				_, err = obsClient.AbortMultipartUpload(&obs.AbortMultipartUploadInput{q.Bucket, fmt.Sprintf("%s%s", filepath.ToSlash(q.Prefix), filepath.ToSlash(event.PathName)), initResult.UploadId})
				if err != nil {
					log.Warningf("Failed to abort multiParts task [%s] due to [%s]", uploadPartInput.UploadId, err)
				}
				return
			}
			if partResult.StatusCode > 300 {
				log.Warningf("Sync fill [%s] failed. Operation:Initialize multi parts. Status Code [%d], RequestID [%s]", event.PathName, initResult.StatusCode, initResult.RequestId)
				_, err = obsClient.AbortMultipartUpload(&obs.AbortMultipartUploadInput{q.Bucket, fmt.Sprintf("%s%s", filepath.ToSlash(q.Prefix), filepath.ToSlash(event.PathName)), initResult.UploadId})
				if err != nil {
					log.Warningf("Failed to abort multiParts task [%s] due to [%s]", uploadPartInput.UploadId, err)
				}
				return
			}
			part := obs.Part{}
			part.PartNumber = partResult.PartNumber
			log.Debugf("Part Num:[%d], Etag:[%s], UploadID:[%s] ", partResult.PartNumber, partResult.ETag, initResult.UploadId)
			//part.Size = uploadPartInput.PartSize
			part.ETag = partResult.ETag
			parts := append(completeParts.Parts, part)
			completeParts.Parts = parts
			log.Debugf("Upload part [%d] successfully. UploadID:[%s], File Name:[%s]", partNum, initResult.UploadId, event.PathName)
		}
		log.Debugf("Total parts need to be completed is [%d], file:[%s]", len(completeParts.Parts), event.PathName)
		res, err := obsClient.CompleteMultipartUpload(completeParts)
		if err != nil {
			log.Warningf("Complete  multiPart failed, UploadID:[%s], File name:[%s]. Due to [%s]", initResult.UploadId, event.PathName, err)
			log.Warningf("Sync file [%s] failed. Due to [%s]", event.PathName, err)
			_, err = obsClient.AbortMultipartUpload(&obs.AbortMultipartUploadInput{q.Bucket, fmt.Sprintf("%s%s", q.Prefix, event.PathName), initResult.UploadId})
			if err != nil {
				log.Warningf("Failed to abort multiParts task [%s] due to [%s]", completeParts.UploadId, err)
			}
			return
		}
		if res.StatusCode > 300 {
			log.Warningf("Sync fill [%s] failed. Operation: Complete multi parts. Status Code [%d], RequestID [%s]", event.PathName, initResult.StatusCode, initResult.RequestId)
			_, err = obsClient.AbortMultipartUpload(&obs.AbortMultipartUploadInput{q.Bucket, fmt.Sprintf("%s%s", q.Prefix, event.PathName), initResult.UploadId})
			if err != nil {
				log.Warningf("Failed to abort multiParts task [%s] due to [%s]", completeParts.UploadId, err)
			}
			return
		}
		log.Infof("Sync file [%s] successfully.", event.PathName)
	}
}

func (q *DataSync) removeSingleFile(obsClient *obs.ObsClient, event *localscanner.DataSyncEvent) {
	q.doRemoveSingFile(obsClient, event.EventTime, fmt.Sprintf("%s%s", filepath.ToSlash(q.Prefix), filepath.ToSlash(event.PathName)))
}

func (q *DataSync) removeDir(obsClient *obs.ObsClient, event *localscanner.DataSyncEvent) {
	//删除远端的文件
	listObjects := new(obs.ListObjectsInput)
	listObjects.Prefix = fmt.Sprintf("%s%s", filepath.ToSlash(q.Prefix), filepath.ToSlash(event.PathName))
	listObjects.Bucket = q.Bucket
	listObjects.MaxKeys = 1000
	isTruncated := true

	for isTruncated {
		listResult, err := obsClient.ListObjects(listObjects)
		if err != nil || listResult.StatusCode != 200 {
			isTruncated = false
			log.Warningf("List objects from bucket [%s] failed, due to [%s] ", err)
			continue
		}
		log.Debugf("List object with prefix:[%s], get objects:[%d]", listResult.Prefix, len(listResult.Contents))
		isTruncated = listResult.IsTruncated
		for _, key := range listResult.Contents {
			q.doRemoveSingFile(obsClient, event.EventTime, key.Key)
		}
		//如果有对象持续删不掉，删不掉的对象超过1000，会进入死循环
	}
	//删除远端的多段任务
	listPartsTask := new(obs.ListMultipartUploadsInput)
	listPartsTask.Prefix = fmt.Sprintf("%s%s", filepath.ToSlash(q.Prefix), filepath.ToSlash(event.PathName))
	listPartsTask.Bucket = q.Bucket
	isTruncated = true
	for isTruncated {
		listResult, err := obsClient.ListMultipartUploads(listPartsTask)
		if err != nil || listResult.StatusCode != 200 {
			isTruncated = false
			log.Warningf("List multi parts tasks from bucket [%s] failed, due to [%s] ", err)
			continue
		}
		log.Debugf("List multi parts tasks with prefix:[%s], get objects:[%d]", listResult.Prefix, len(listResult.Uploads))
		isTruncated = listResult.IsTruncated
		for _, upload := range listResult.Uploads {
			_, err = obsClient.AbortMultipartUpload(&obs.AbortMultipartUploadInput{q.Bucket, upload.Key, upload.UploadId})
			if err != nil {
				log.Warningf("Failed to abort multiParts task [%s] for key [%s] due to [%s].", upload.UploadId, upload.Key, err)
			}
		}
		//如果有对象持续删不掉，删不掉的对象超过1000，会进入死循环
	}

}

func (q *DataSync) doRemoveSingFile(obsClient *obs.ObsClient, eventTime time.Time, removePath string) {
	deleteObject := &obs.DeleteObjectInput{}
	deleteObject.Key = removePath
	deleteObject.Bucket = q.Bucket
	delRes, err := obsClient.DeleteObject(deleteObject)
	if err != nil {
		log.Warningf("Remove file [%s] failed due to [%s]", removePath, err)
		return
	}
	if delRes.StatusCode == 404 {
		log.Info("Remove file [%s] successfully.")
		return
	}
	//查询远端对象的元数据
	//getObjectMetadataInput := &obs.GetObjectMetadataInput{}
	//getObjectMetadataInput.Bucket = q.Bucket
	//getObjectMetadataInput.Key = removePath
	//
	//result, err := obsClient.GetObjectMetadata(getObjectMetadataInput)
	//if err != nil {
	//	log.Warningf("Remove file [%s] failed while head object meta, due to [%s]", removePath, err)
	//	return
	//}
	////远端查不到对象，不用删除
	//if result.StatusCode == 404 {
	//	log.Warningf("Remote file [%s] does not exist.", removePath, err)
	//	return
	//} else if result.StatusCode == 200 { //远端查到对象，需要判断远端对象的x-obs-meta-event-ns内的时间是否早于当前event的时间，如果时间早于event则删除，否则不删除（避免误删）
	//	//value, ok := result.Metadata[EVENTNS]
	//	//if !ok {
	//	//	//远端对象里没有x-obs-meta-event-ns自定义元数据，不删除对象
	//	//	log.Warningf("Remote file [%s] does not have [%s], do not remove. file:[%s]", EVENTNS, removePath)
	//	//	return
	//	//}
	//	//event_ns, err := strconv.Atoi(value)
	//	//if err != nil {
	//	//	log.Warningf("Remote file [%s] has invalid %s, do not remove. value:[%s], file:[%s]", EVENTNS, value, removePath)
	//	//	return
	//	//}
	//	//远端对象时间比当前event时间晚，不删除
	//	//if eventTime.Unix() < int64(event_ns) {
	//	//	log.Warningf("Remote event time [%s] is newer than current event time [%d], do not remove. value:[%s], file:[%s]", EVENTNS, eventTime.Nanosecond(), value, removePath)
	//	//	return
	//	//}
	//	//远端对象时间早于当前event时间，删除
	//	deleteObject := &obs.DeleteObjectInput{}
	//	deleteObject.Key = removePath
	//	deleteObject.Bucket = q.Bucket
	//	delRes, err := obsClient.DeleteObject(deleteObject)
	//	if err != nil {
	//		log.Warningf("Remove file [%s] failed due to [%s]", removePath, err)
	//		return
	//	}
	//	if delRes.StatusCode == 404 {
	//		log.Info("Remove file [%s] successfully.")
	//		return
	//	}
	//	//其他非预期内的返回码，报错
	//	log.Warningf("Remove file [%s] failed due to [%s], requestID:[%s]", removePath, delRes.StatusCode, delRes.RequestId)
	//	return
	//
	//} else { //查询远端对象失败，无法删除
	//	log.Warningf("Remove file [%s] failed due to head remote object failed [%s]", removePath, err)
	//}
}

func (q *DataSync) enableRoutine() {
	q.goRoutinesChan <- 0
}
