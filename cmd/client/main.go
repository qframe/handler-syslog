package main

import (
	"log"
	"github.com/zpatrick/go-config"
	"github.com/qframe/collector-file"
	"os"
	"github.com/qframe/types/qchannel"
	"github.com/qframe/handler-syslog"
	"github.com/qframe/types/messages"
	"github.com/qframe/filter-grok"
	"reflect"
	"time"
)

func checkErr(pname string, err error) {
	if err != nil {
		log.Printf("[EE] Failed to create %s plugin: %s", pname, err.Error())
		os.Exit(1)
	}
}

func main() {
	qChan := qtypes_qchannel.NewQChan()
	qChan.Broadcast()
	if len(os.Args) != 2 {
		log.Fatal("usage: ./client <path>")

	}
	fPath := os.Args[1]
	cfgMap := map[string]string{
		"log.level": "debug",
		"collector.file.path": fPath,
		"collector.file.reopen": "false",
		"filter.grok.inputs": "file",
		"filter.grok.pattern": "%{WORD:tag} %{WORD:severity} %{GREEDYDATA:message}",
		"handler.syslog.inputs": "grok",
		"handler.syslog.default-tag": "test",
		"handler.syslog.addr": "tasks.rsyslog:514",
	}
	cfg := config.NewConfig([]config.Provider{config.NewStatic(cfgMap)})
	// Syslog handler
	phs, err := qhandler_syslog.New(qChan, cfg, "syslog")
	checkErr(phs.Name, err)
	go phs.Run()
	// GROK
	pfm, err := qfilter_grok.New(qChan, cfg, "grok")
	checkErr(pfm.Name, err)
	go pfm.Run()
	time.Sleep(time.Duration(500)*time.Millisecond)
	p, _ := qcollector_file.New(qChan, cfg, "file")
	go p.Run()
	bg := p.QChan.Data.Join()
	log.Println("Start receive loop...")
	for {
		val := <- bg.Read
		switch val.(type) {
		case qtypes_messages.Message:
			continue
		default:
			log.Printf("Dunno time | %s", reflect.TypeOf(val))
		}

	}
}
