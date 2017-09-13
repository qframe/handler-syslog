package qhandler_syslog

import (
	"fmt"
	"reflect"
	"github.com/RackSec/srslog"
	"github.com/zpatrick/go-config"

	"github.com/qframe/types/qchannel"
	"github.com/qframe/types/plugin"
	"github.com/qframe/types/constants"
	"github.com/qframe/types/messages"
	"sync"
)

const (
	version   = "0.0.0"
	pluginTyp = qtypes_constants.HANDLER
	pluginPkg = "syslog"
)

type Plugin struct {
	*qtypes_plugin.Plugin
	mu 			sync.Mutex
	logWriter 	map[string]*srslog.Writer
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
	return p, nil
}
func (p *Plugin) GetWriter(kv map[string]string) (w *srslog.Writer, err error) {
	// <facility>
	facility, fOk := kv["facility"]
	if !fOk {
		msg := fmt.Sprintf("facility are not found in map '%v'", kv)
		p.Log("error", msg)
		return w, fmt.Errorf(msg)
	}
	key := fmt.Sprintf("%s", facility)
	p.mu.Lock()
	defer p.mu.Unlock()
	w, ok := p.logWriter[key]
	if ! ok {
		w, err = srslog.Dial(p.proto,p.addr, srslog.LOG_INFO, facility)
		p.Log("debug", fmt.Sprintf("Create new writer '%s'", key))
		p.logWriter[key] = w
	}
	return
}

func (p *Plugin) LogLine(w *srslog.Writer, kv map[string]string, log string) (err error) {
	severity, fOk := kv["severity"]
	if !fOk {
		msg := fmt.Sprintf("severity are not found in map '%v'", kv)
		p.Log("error", msg)
		return fmt.Errorf(msg)
	}

	switch severity {
	case "error":
		w.Err(log)
	case "warn":
		w.Err(log)
	case "notice":
		w.Err(log)
	case "info":
		w.Err(log)
	case "debug":
		w.Err(log)
	default:
		err = fmt.Errorf("unkonwn severity '%s' (allowd: error|warn|notice|info|debug)", severity)
	}
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
				p.LogLine(w, qm.Tags, qm.Message)
			default:
				p.Log("info", fmt.Sprintf("Dunno type '%s': %v", reflect.TypeOf(val), val))
			}
		}
	}
}
