package config

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/go-xweb/log"
)

type ConfigGroup interface {
	Get(key string) string
}

type IObserver func(key, value string)

type GeneralConfigGroup struct {
	configMap           map[string]string
	lock                *sync.RWMutex
	internalConfigGroup ConfigGroup
	watchs              []IObserver
}

func NewGeneralConfigGroup(internalConfigGroup ConfigGroup) *GeneralConfigGroup {
	return &GeneralConfigGroup{
		lock:                &sync.RWMutex{},
		internalConfigGroup: internalConfigGroup,
		configMap:           make(map[string]string),
		watchs:              make([]IObserver, 0),
	}
}

func (g *GeneralConfigGroup) get(key string) string {
	g.lock.RLock()
	defer g.lock.RUnlock()

	value, ok := g.configMap[key]
	if !ok {
		return ``
	}

	return value
}

//获取string类型的属性
func (g *GeneralConfigGroup) Get(key string) string {
	value := g.get(key)

	if len(value) > 0 {
		return value
	}

	if g.internalConfigGroup == nil {
		return ``
	}

	value = g.internalConfigGroup.Get(key)

	g.put(key, value)

	return value
}

//获取int类型的属性
func (g *GeneralConfigGroup) GetInt(key string) (int, error) {
	value := g.Get(key)
	result, err := strconv.Atoi(value)
	if err != nil {
		log.Printf(`config error: name:%s, value:%s`, key, value)
		return -1, err
	}

	return result, nil
}

// 获取bool类型的属性
// 当属性值为 1, 1.0, t, T, TRUE, true, True, YES, yes, Yes,Y, y, ON, on, On 时,返回true
// 当属性值为 0, 0.0, f, F, FALSE, false, False, NO, no, No, N,n, OFF, off, Off 时,返回false
// 否则返回错误
func (g *GeneralConfigGroup) GetBool(key string) (bool, error) {
	val := g.Get(key)
	if len(val) > 0 {
		val = strings.ToLower(val)
		switch val {
		case "1", "t", "true", "yes", "y", "on":
			return true, nil
		case "0", "f", "false", "no", "n", "off":
			return false, nil
		}
	}

	return false, fmt.Errorf("parsing %q: invalid syntax", val)
}

//设置一个属性集合
func (g *GeneralConfigGroup) PutAll(configs map[string]string) {
	if configs != nil && len(configs) > 0 {
		for key, value := range configs {
			g.Put(key, value)
		}
	}
}

func (g *GeneralConfigGroup) put(key, value string) string {
	g.lock.Lock()
	defer g.lock.Unlock()

	g.configMap[key] = value
	return value
}

//设置一个属性
func (g *GeneralConfigGroup) Put(key, value string) string {
	if len(key) == 0 {
		return ``
	}

	preValue := g.Get(key)

	if preValue == value {
		return value
	}

	value = g.put(key, value)

	g.notify(key, value)

	return value
}

func (g *GeneralConfigGroup) size() int {
	g.lock.RLock()
	defer g.lock.RUnlock()
	return len(g.configMap)
}

func (g *GeneralConfigGroup) clone() map[string]string {
	g.lock.RLock()
	defer g.lock.RUnlock()
	clone := make(map[string]string, len(g.configMap))
	for key, value := range g.configMap {
		clone[key] = value
	}
	return clone
}

//遍历属性并进行回调
func (g *GeneralConfigGroup) ForEach(callback func(key, value string)) {
	for key, value := range g.clone() {
		callback(key, value)
	}
}

//添加属性变化监听器,当此监听器所关心的属性发生变化时,会调用此监听器所定义的回调函数
func (g *GeneralConfigGroup) AddWatcher(watch IObserver) {
	g.watchs = append(g.watchs, watch)
}

func (g *GeneralConfigGroup) notify(key, value string) {
	for _, observer := range g.watchs {
		go func() {
			observer(key, value)
		}()
	}
}
