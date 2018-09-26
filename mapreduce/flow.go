package mapreduce

import (
	"errors"

	"time"

	"gitlab.x.lan/application/droplet-app/pkg/mapper/consolelog"
	"gitlab.x.lan/application/droplet-app/pkg/mapper/flow"
	"gitlab.x.lan/application/droplet-app/pkg/mapper/flowtype"
	"gitlab.x.lan/application/droplet-app/pkg/mapper/geo"
	"gitlab.x.lan/application/droplet-app/pkg/mapper/perf"
	"gitlab.x.lan/application/droplet-app/pkg/mapper/platform"
	"gitlab.x.lan/yunshan/droplet-libs/app"
	"gitlab.x.lan/yunshan/droplet-libs/datatype"
	"gitlab.x.lan/yunshan/droplet-libs/queue"
	"gitlab.x.lan/yunshan/droplet-libs/stats"
)

const (
	perfSlots = 2
)

const GEO_FILE_LOCATION = "/usr/share/droplet/ip_info_mini.json"

func NewFlowMapProcess(inputQueue queue.Queue, outputQueue queue.QueueWriter) *FlowHandler {
	flowMapProcess := NewFlowHandler([]app.FlowProcessor{
		flow.NewProcessor(),
		perf.NewProcessor(),
		geo.NewProcessor(GEO_FILE_LOCATION),
		flowtype.NewProcessor(),
		consolelog.NewProcessor(),
		platform.NewProcessor(),
	}, outputQueue)

	go func() {
		for range time.NewTicker(time.Minute).C {
			inputQueue.Put(nil)
		}
	}()
	go func() {
		elements := make([]interface{}, QUEUE_BATCH_SIZE)
		for {
			n := inputQueue.Gets(elements)
			for _, e := range elements[:n] {
				if e == nil { // tick
					if flowMapProcess.NeedFlush() {
						flowMapProcess.Flush()
					}
					continue
				}

				flowMapProcess.Process(e.(*datatype.TaggedFlow))
			}
		}
	}()

	return flowMapProcess
}

type flowAppStats struct {
	EpcEmitCounter uint64 `statsd:"epc_doc"`
	//TO DO: add other apps

}

type FlowHandler struct {
	numberOfApps int
	processors   []app.FlowProcessor
	stashes      []*Stash

	lastProcess time.Duration

	zmqAppQueue queue.QueueWriter

	emitCounter  []uint64
	counterLatch int
	statItems    []stats.StatItem
}

func NewFlowHandler(processors []app.FlowProcessor, zmqAppQueue queue.QueueWriter) *FlowHandler {
	nApps := len(processors)
	handler := FlowHandler{
		numberOfApps: nApps,
		processors:   processors,
		stashes:      make([]*Stash, nApps),
		lastProcess:  time.Duration(time.Now().UnixNano()),
		zmqAppQueue:  zmqAppQueue,
		emitCounter:  make([]uint64, nApps*2),
		counterLatch: 0,
		statItems:    make([]stats.StatItem, nApps),
	}
	for i := 0; i < handler.numberOfApps; i++ {
		processors[i].Prepare()
		handler.stashes[i] = NewStash(DOCS_IN_BUFFER, WINDOW_SIZE)
		handler.statItems[i].Name = processors[i].GetName()
		handler.statItems[i].StatType = stats.COUNT_TYPE
	}
	stats.RegisterCountable("flow_mapper", &handler)
	return &handler
}

func (f *FlowHandler) GetCounter() interface{} {
	oldLatch := f.counterLatch
	if f.counterLatch == 0 {
		f.counterLatch = f.numberOfApps
	} else {
		f.counterLatch = 0
	}
	for i := 0; i < f.numberOfApps; i++ {
		f.statItems[i].Value = f.emitCounter[i+oldLatch]
		f.emitCounter[i+oldLatch] = 0
	}
	return f.statItems
}

func (f *FlowHandler) putToQueue() {
	for i, stash := range f.stashes {
		docs := stash.Dump()
		for j := 0; j < len(docs); j += QUEUE_BATCH_SIZE {
			if j+QUEUE_BATCH_SIZE <= len(docs) {
				f.zmqAppQueue.Put(docs[j : j+QUEUE_BATCH_SIZE]...)
			} else {
				f.zmqAppQueue.Put(docs[j:]...)
			}
		}
		f.emitCounter[i+f.counterLatch] += uint64(len(docs))
		stash.Clear()
	}
}

func isValidFlow(flow *datatype.TaggedFlow) bool {
	startTime := flow.StartTime
	endTime := flow.EndTime
	curTime := time.Duration(time.Now().UnixNano())

	if startTime > curTime || endTime > curTime {
		return false
	}
	if endTime > startTime+2*time.Minute {
		return false
	}
	if endTime != 0 && endTime < startTime {
		return false
	}

	currStart := flow.CurStartTime
	arr0Last := flow.FlowMetricsPeerSrc.ArrTimeLast
	arr1Last := flow.FlowMetricsPeerDst.ArrTimeLast
	rightMargin := endTime + 2*time.Minute
	times := [3]time.Duration{currStart, arr0Last, arr1Last}
	for i := 0; i < 3; i++ {
		if times[i] > rightMargin || times[i] > curTime {
			return false
		}
	}
	return true
}

func (f *FlowHandler) Process(flow *datatype.TaggedFlow) error {
	if !isValidFlow(flow) {
		return errors.New("flow timestamp incorrect and droped")
	}

	for i, processor := range f.processors {
		docs := processor.Process(flow, false)
		for {
			docs = f.stashes[i].Add(docs)
			if docs == nil {
				break
			}
			f.Flush()
		}
	}

	f.lastProcess = time.Duration(time.Now().UnixNano())
	return nil
}

func (f *FlowHandler) Flush() {
	f.putToQueue()
	f.lastProcess = time.Duration(time.Now().UnixNano())
}

func (f *FlowHandler) NeedFlush() bool {
	return time.Duration(time.Now().UnixNano())-f.lastProcess > time.Minute
}
