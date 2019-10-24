package util

import (
	"fmt"
	"github.com/Unknwon"
	"github.com/sirupsen/logrus"
	"log"
	"strconv"
)

func GetConfigValueString(cfg *goconfig.ConfigFile, section, key string) (string, error) {
	if cfg == nil {
		return "", fmt.Errorf("Null pointer error")
	}
	value, err := cfg.GetValue(section, key)
	if err != nil {
		return "", fmt.Errorf("Failed to load [%s/%s] due to [%s]", section, key, err)
	}
	return value, nil
}

func GetConfigValueInt(cfg *goconfig.ConfigFile, section, key string) (int, error) {
	if cfg == nil {
		return -1, fmt.Errorf("Null pointer error")
	}
	value, err := cfg.GetValue(section, key)
	if err != nil {
		return -1, fmt.Errorf("Failed to load [%s/%s] due to [%s]", section, key, err)
	}
	valueInt, err := strconv.Atoi(value)
	if err != nil {
		return -1, fmt.Errorf("Failed to load [%s/%s] while convert string to int, due to [%s]", section, key, err)
	}
	return valueInt, nil
}

func GetConfigValueBool(cfg *goconfig.ConfigFile, section, key string) (bool, error) {
	if cfg == nil {
		return false, fmt.Errorf("Null pointer error")
	}
	value, err := cfg.GetValue(section, key)
	if err != nil {
		return false, fmt.Errorf("Failed to load [%s/%s] due to [%s]", section, key, err)
	}
	valueInt, err := strconv.ParseBool(value)
	if err != nil {
		log.Fatalf("Failed to load [%s/%s] while convert string to bool, due to [%s]", section, key, err)
	}
	return valueInt, nil
}

func GetLogLevel(cfg *goconfig.ConfigFile) logrus.Level {
	lvlStr, err := GetConfigValueString(cfg, "log", "LogLevel")
	if err != nil {
		return logrus.InfoLevel
	}
	switch lvlStr {
	case "DEBUG":
		return logrus.DebugLevel
	case "INFO":
		return logrus.InfoLevel
	case "WARNING":
		return logrus.WarnLevel
	case "ERROR":
		return logrus.ErrorLevel
	case "FATAL":
		return logrus.FatalLevel
	default:
		return logrus.InfoLevel
	}
}
