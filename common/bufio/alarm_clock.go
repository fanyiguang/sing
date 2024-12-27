package bufio

import (
	"github.com/fanyiguang/brick/channel"
	"sync"
	"time"
)

type AlarmClock struct {
	sync.Once
	finish func()
	timer  chan struct{}
	done   chan struct{}
}

func NewAlarmClock(f func()) *AlarmClock {
	return &AlarmClock{
		timer:  make(chan struct{}, 1),
		done:   make(chan struct{}),
		finish: f,
	}
}

func (a *AlarmClock) Start(timeout int) {
	a.Do(func() {
		go a.start(timeout)
	})
}

func (a *AlarmClock) start(timeout int) {
	ticker := time.NewTicker(time.Duration(timeout) * time.Second)
	for {
		select {
		case <-ticker.C:
			_, err := channel.NonBlockAccept(a.timer)
			if err != nil {
				a.finish()
				ticker.Stop()
				return
			}
		case <-a.done:
			return
		}
	}
}

func (a *AlarmClock) ResetTimer() {
	_ = channel.NonBlockSend(a.timer, struct{}{})
}

func (a *AlarmClock) Close() {
	channel.Close(a.done)
}
