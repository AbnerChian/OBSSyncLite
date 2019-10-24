package logmgr

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/syhlion/logrus-hook-caller/logcaller"
	"os"
	"sync"
)

var initOnce sync.Once
var logger *logrus.Entry
var LogConfig = "./config/log.conf"
var LogFile = "./logs/obssynclite.log"

func NewLogger() *logrus.Entry {
	initOnce.Do(func() {
		log := logrus.New()
		out, err := os.OpenFile(LogFile, os.O_APPEND|os.O_CREATE, 0755)
		if err != nil {
			panic(fmt.Sprintf("Failed to open log file:[%s] due to [%s]", LogFile, err))
		}
		log.Out = out

		//CallerHook
		callerHook := &logcaller.CallerHook{}
		log.AddHook(callerHook)
		logger = log.WithField("app", "OBSSyncLite")
	})
	return logger
}
