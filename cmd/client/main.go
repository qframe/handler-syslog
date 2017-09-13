package main

import (
	"log"
	"github.com/zpatrick/go-config"
	"github.com/qframe/collector-file"
	"os"
	"github.com/qframe/types/qchannel"
	"github.com/qframe/handler-syslog"
	"github.com/qframe/types/messages"
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
		"log.level": "trace",
		"log.only-plugins": "grok,syslog",
		"collector.file.path": fPath,
		"collector.file.reopen": "false",
		"filter.grok.inputs": "file",
		"filter.grok.pattern": "%{WORD:facility}|%{GREEDYDATA:message}",
		"handler.syslog.inputs": "file",
		"handler.syslog.default-severity": "info",
		"handler.syslog.default-tag": "test",
		"handler.syslog.addr": "tasks.rsyslog:514",
	}
	cfg := config.NewConfig([]config.Provider{config.NewStatic(cfgMap)})

	p, _ := qcollector_file.New(qChan, cfg, "file")
	go p.Run()
	/*
	// GROK
	pfm, err := qfilter_grok.New(qChan, cfg, "grok")
	checkErr(pfm.Name, err)
	go pfm.Run()
	*/
	phs, err := qhandler_syslog.New(qChan, cfg, "syslog")
	checkErr(phs.Name, err)
	go phs.Run()
	bg := p.QChan.Data.Join()
	log.Println("Start receive loop...")
	for {
		val := <- bg.Read
		switch val.(type) {
		case qtypes_messages.Message:
			qm := val.(qtypes_messages.Message)
			if qm.GetLastSource() == "file" && qm.SourceSuccess {
				continue
				//log.Printf("%v | %s", qm.Tags, qm.Message)
			}
		}

	}
}
