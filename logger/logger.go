package logger

import (
	"os"
	"strings"

	"github.com/sirupsen/logrus"
)

var log = logrus.New()

func init() {
	if strings.ToLower(os.Getenv("LOGLEVEL")) == "production" {
		log.Formatter = new(logrus.JSONFormatter)
	}

	log.SetReportCaller(true)
}

func GetLogger() *logrus.Logger {
	switch strings.ToLower(os.Getenv("LOGLEVEL")) {
	case "trace":
		log.Level = logrus.TraceLevel
	case "error":
		log.Level = logrus.ErrorLevel
	case "warn":
		log.Level = logrus.WarnLevel
	case "info":
		log.Level = logrus.InfoLevel
	default:
		log.Level = logrus.DebugLevel
	}

	return log
}
