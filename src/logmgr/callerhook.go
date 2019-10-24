package logmgr

import (
	"github.com/sirupsen/logrus"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

type CallerHooker struct {
	ParentLogger *logrus.Logger
}

func New(ParentLogger *logrus.Logger) *CallerHooker {
	return &CallerHooker{
		ParentLogger: ParentLogger,
	}
}

func (q *CallerHooker) Fire(entry *logrus.Entry) error {

	entry.Data["caller"] = q.caller(5)
	return nil
}

func (q *CallerHooker) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
	}
}

func (q *CallerHooker) caller(skip int) string {
	if _, file, line, ok := runtime.Caller(skip); ok {
		return strings.Join([]string{filepath.Base(file), strconv.Itoa(line)}, ":")
	}
	return "?"
}
