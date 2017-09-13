package qcollector_file

import (
	"log"
	"os"
	"github.com/hpcloud/tail"
	"github.com/zpatrick/go-config"
	"github.com/qframe/types/qchannel"
	"github.com/qframe/types/plugin"
	"github.com/qframe/types/messages"
)

const (
	version = "0.2.0"
	pluginTyp = "collector"
	pluginPkg = "file"
)

type Plugin struct {
	*qtypes_plugin.Plugin
}

func New(qChan qtypes_qchannel.QChan, cfg *config.Config, name string) (Plugin, error) {
	return Plugin{
		Plugin: qtypes_plugin.NewNamedPlugin(qChan, cfg, pluginTyp, pluginPkg, name, version),
	}, nil
}

func (p *Plugin) Run() {
	log.Printf("[II] Start collector v%s", version)
	fPath, err := p.CfgString("path")
	if err != nil {
		log.Println("[EE] No file path for collector.file.path set")
		return
	}
	create := p.CfgBoolOr("create", false)
	if _, err := os.Stat(fPath); os.IsNotExist(err) && create {
		log.Printf("[DD] Create file: %s", fPath)
		f, _ := os.Create(fPath)
		f.Close()
	}
	fileReopen := p.CfgBoolOr("reopen", true)
	t, err := tail.TailFile(fPath, tail.Config{Follow: true, ReOpen: fileReopen})
	if err != nil {
		log.Printf("[WW] File collector failed to open %s: %s", fPath, err)
	}
	b := qtypes_messages.NewBase(p.Name)
	for line := range t.Lines {
		p.Log("trace", line.Text)
		qm := qtypes_messages.NewMessage(b, line.Text)
		p.QChan.SendData(qm)
	}
}

