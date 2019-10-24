package localscanner

import (
	"github.com/radovskyb/watcher"
	"time"
)

/*
Key：目录全量路径名
Value： DirSync 记录目录当前的同步状态，以及整个目录从哪个目录copy、move过来的：PreviousName
*/

type FSEvent struct {
	watcher.Event
	EventTime time.Time
}

type DirSync struct {
	PreviousName string
	SyncStatus   SyncStat
}

type SyncStat int

const (
	Create SyncStat = iota
	Sync
	Renamed
	Moved
	Copied
	Deleted
)

type FileExist int

const (
	IsFile FileExist = iota
	IsDirectory
	NoExist
)

type SyncOpreation int

const (
	FileSync SyncOpreation = iota
	FileRemove
	DirRemove
)

type DataSyncEvent struct {
	PathName  string
	IsDir     bool
	Operation SyncOpreation
	EventTime time.Time
}
