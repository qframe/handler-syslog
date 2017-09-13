package qhandler_syslog

import (
	"fmt"
	"log/syslog"
	"sync"
	"reflect"
	"github.com/zpatrick/go-config"

	"github.com/qframe/types/qchannel"
	"github.com/qframe/types/plugin"
	"github.com/qframe/types/constants"
	"github.com/qframe/types/messages"
)

const (
	version   = "0.0.0"
	pluginTyp = qtypes_constants.HANDLER
	pluginPkg = "syslog"
)

type Plugin struct {
	*qtypes_plugin.Plugin
	mu 			sync.Mutex
	defTag string
	defSeverity string
	logWriter 	map[string]*syslog.Writer
	addr,proto 	string
}

func New(qChan qtypes_qchannel.QChan, cfg *config.Config, name string) (Plugin, error) {
	p := Plugin{
		Plugin: qtypes_plugin.NewNamedPlugin(qChan, cfg, pluginTyp, pluginPkg, name, version),
	}
	p.Version = version
	p.Name = name
	p.addr = p.CfgStringOr("addr", "127.0.0.1:514")
	p.proto = p.CfgStringOr("proto", "udp")
	p.defTag = p.CfgStringOr("default-tag", "")
	p.defSeverity = p.CfgStringOr("default-severity", "")
	p.logWriter = map[string]*syslog.Writer{}
	return p, nil
}

func (p *Plugin) GetWriter(kv map[string]string) (w *syslog.Writer, err error) {
	// <tag>
	tag, fOk := kv["tag"]
	if !fOk {
		msg := fmt.Sprintf("tag are not found in map '%v'", kv)
		p.Log("error", msg)
		return w, fmt.Errorf(msg)
	}
	key := fmt.Sprintf("%s", tag)
	p.mu.Lock()
	defer p.mu.Unlock()
	w, ok := p.logWriter[key]
	if !ok {
		w, err = syslog.Dial(p.proto,p.addr, syslog.LOG_INFO|syslog.LOG_LOCAL7, tag)
		if err != nil {
			return
		}
		p.Log("debug", fmt.Sprintf("Create new writer '%s'", key))
		p.logWriter[key] = w
		return w, nil
	}
	return
}

func (p *Plugin) LogLine(w *syslog.Writer, kv map[string]string, str string) (err error) {
	severity, fOk := kv["severity"]
	if !fOk {
		msg := fmt.Sprintf("severity are not found in map '%v'", kv)
		p.Log("error", msg)
		return fmt.Errorf(msg)
	}
	message, ok := kv["message"]
	if ! ok {
		message = str
	}
	switch severity {
	case "error":
		w.Err(message)
	case "warn":
		w.Warning(message)
	case "notice":
		w.Notice(message)
	case "info":
		w.Info(message)
	case "debug":
		w.Debug(message)
	default:
		err = fmt.Errorf("unkonwn severity '%s' (allowd: error|warn|notice|info|debug)", severity)
	}
	p.Log("trace", fmt.Sprintf("logged [%s] %s", severity, message))
	return
}

// Run fetches everything from the Data channel and flushes it to stdout
func (p *Plugin) Run() {
	p.Log("notice", fmt.Sprintf("Start %s %s-handler v%s", p.Name, p.Pkg, p.Version))
	bg := p.QChan.Data.Join()
	for {
		select {
		case val := <-bg.Read:
			switch val.(type) {
			case qtypes_messages.Message:
				qm := val.(qtypes_messages.Message)
				if qm.StopProcessing(p.Plugin, false) {
					continue
				}
				w, err := p.GetWriter(qm.Tags)
				if err != nil {
					p.Log("error", err.Error())
					continue
				}
				err = p.LogLine(w, qm.Tags, qm.Message)
				if err != nil {
					p.Log("error", err.Error())
				}
			default:
				p.Log("info", fmt.Sprintf("Dunno type '%s': %v", reflect.TypeOf(val), val))
			}
		}
	}
}
