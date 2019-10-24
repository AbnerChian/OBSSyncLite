package main

import (
	"./datacheck"
	"./datasync"
	"./localscanner"
	"./logmgr"
	"./util"
	"github.com/Unknwon"
	"github.com/radovskyb/watcher"
	"github.com/sirupsen/logrus"
	"os"
	"runtime"
	"strings"
	"time"
)

const (
	ConfigFile = "./config/obssynclite.conf"
	QueueSize  = 1024 * 1024
)

var (
	cfg *goconfig.ConfigFile
	log *logrus.Entry
)

func init() {
	log = logmgr.NewLogger()

	config, err := goconfig.LoadConfigFile(ConfigFile)
	if err != nil {
		log.Errorln("Failed to load ConfigFile Information!")
		os.Exit(-1)
	}
	log.Infoln("Load LogConfig successfully!")
	cfg = config
}

func main() {
	//从配置文件里价值配置信息
	scanner := &localscanner.LocalScanner{}

	//获取监控的目录信息
	pathString, err := cfg.GetValue("sync", "LocalDirs")
	if err != nil {
		log.Fatalf("Failed to load LocalDirs due to [%s]", err)
	}
	log.Debugf("LocalDir is [%s].", pathString)
	paths := strings.Split(pathString, ",")
	log.Debugf("paths are [%v]", paths)

	scanner.ScanPaths = paths

	scanner.QueueSize = QueueSize

	scanner.EventsChan = make(chan *localscanner.FSEvent, scanner.QueueSize)

	w := watcher.New()
	w.FilterOps(watcher.Create, watcher.Write, watcher.Remove, watcher.Rename, watcher.Move)

	scanner.FSWatcher = w

	syncer := &datasync.DataSync{}
	syncer.EndPoint = secGetConfigValueString(cfg, "connection", "EndPoint")
	syncer.AK = secGetConfigValueString(cfg, "connection", "AK")
	syncer.SK = secGetConfigValueString(cfg, "connection", "SK")
	syncer.IsHTTPS = secGetConfigValueBool(cfg, "connection", "IsHttps")
	syncer.IsLongConnection = secGetConfigValueBool(cfg, "connection", "IsLongConnection")
	syncer.Concurrency = secGetConfigValueInt(cfg, "connection", "Concurrency")
	syncer.Bucket = secGetConfigValueString(cfg, "connection", "Bucket")
	rawSync := secGetConfigValueString(cfg, "sync", "Prefix")
	switch runtime.GOOS {
	case "windows":
		syncer.Prefix = strings.TrimRight(rawSync, "/")
		syncer.Prefix = syncer.Prefix + "/"
	case "linux":
		syncer.Prefix = strings.TrimRight(rawSync, "/")
	default:
		syncer.Prefix = rawSync
	}
	syncer.PartSize = int64(secGetConfigValueInt(cfg, "sync", "PartSize"))
	syncer.DataSyncChan = make(chan *localscanner.DataSyncEvent, scanner.QueueSize)

	//启动文件系统事件监听
	go scanner.DoDetecting()
	//启动处理文件系统事件处理
	go scanner.EventHandler(syncer.DataSyncChan)

	if secGetConfigValueBool(cfg, "sync", "IsFullSync") {
		for _, dir := range scanner.ScanPaths {
			scanner.FullDirScan(dir, syncer.DataSyncChan)
		}
	}

	//启动文件同步协程
	go syncer.StartSync()
	for _, dir := range scanner.ScanPaths {
		err := w.AddRecursive(dir)
		if err != nil {
			log.Fatalf("Failed to add dir [%s] to watcher!", dir)
		}
	}

	//启动后台扫描
	dataCheck := &datacheck.DataCheck{}
	checkInterval := secGetConfigValueInt(cfg, "sync", "DataCheckInterval")
	dataCheck.CheckInterval = time.Second * time.Duration(checkInterval)
	dataCheck.CheckMethod = datacheck.LocalFirst
	dataCheck.IsCheckMD5 = false
	dataCheck.LocalDirs = scanner.ScanPaths
	dataCheck.RemotePrefix = syncer.Prefix
	dataCheck.Bucket = syncer.Bucket
	dataCheck.EndPoint = syncer.EndPoint
	dataCheck.AK = syncer.AK
	dataCheck.SK = syncer.SK
	dataCheck.CheckConcurrency = secGetConfigValueInt(cfg, "sync", "DataCheckConcurrency")
	dataCheck.DataSyncChan = syncer.DataSyncChan
	dataCheck.CheckEventChan = make(chan *datacheck.CheckEvent, QueueSize)

	if secGetConfigValueBool(cfg, "sync", "DataCheckOpen") {
		go dataCheck.StartCheck()
	}

	watcherInterval := secGetConfigValueInt(cfg, "sync", "FSCheckInterval")
	err = w.Start(time.Microsecond * time.Duration(watcherInterval))
	if err != nil {
		log.Fatalf("Failed to start FS watcher due to [%s]!", err)
	}

}

func secGetConfigValueString(cfg *goconfig.ConfigFile, section, key string) string {
	value, err := util.GetConfigValueString(cfg, section, key)
	if err != nil {
		log.Fatalf("Failed to load config [%s/%s] due to [%s]", section, key, err)
	}
	return value
}

func secGetConfigValueInt(cfg *goconfig.ConfigFile, section, key string) int {
	value, err := util.GetConfigValueInt(cfg, section, key)
	if err != nil {
		log.Fatalf("Failed to load config [%s/%s] due to [%s]", section, key, err)
	}
	return value
}

func secGetConfigValueBool(cfg *goconfig.ConfigFile, section, key string) bool {
	value, err := util.GetConfigValueBool(cfg, section, key)
	if err != nil {
		log.Fatalf("Failed to load config [%s/%s] due to [%s]", section, key, err)
	}
	return value
}
