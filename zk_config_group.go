package config

import (
	"log"
	"time"

	"github.com/yxdrlitao/curator"
	"github.com/yxdrlitao/go-zookeeper/zk"
)

type ZookeeperConfigGroup struct {
	configProfile    *ZookeeperConfigProfile
	node             string //节点名字
	client           curator.CuratorFramework
	configLocalCache *ConfigLocalCache
	*GeneralConfigGroup
}

//新Zk配置组
func NewZookeeperConfigGroup(configProfile *ZookeeperConfigProfile, node string) (*ZookeeperConfigGroup, error) {
	group := &ZookeeperConfigGroup{
		configProfile:      configProfile,
		node:               node,
		GeneralConfigGroup: NewGeneralConfigGroup(nil),
	}
	err := group.initConfigs()
	if err != nil {
		return nil, err
	}

	return group, nil
}

//新Zk配置组,并缓存到本地
func NewZookeeperConfigGroupWithCache(configProfile *ZookeeperConfigProfile, node string, cachePath string) (*ZookeeperConfigGroup, error) {
	group := &ZookeeperConfigGroup{
		configProfile:      configProfile,
		node:               node,
		configLocalCache:   newConfigLocalCache(cachePath),
		GeneralConfigGroup: NewGeneralConfigGroup(nil),
	}

	err := group.initConfigs()
	if err != nil {
		return nil, err
	}

	return group, nil
}

/**
 * 初始化节点
 */
func (g *ZookeeperConfigGroup) initConfigs() error {

	builder := &curator.CuratorFrameworkBuilder{
		ConnectionTimeout: 1 * time.Second,
		SessionTimeout:    1 * time.Second,
		RetryPolicy:       g.configProfile.RetryPolicy,
	}

	g.client = builder.ConnectString(g.configProfile.ConnectStr).Build()

	// g is one method of getting event/async notifications
	err := g.client.Start()
	if err != nil {
		log.Printf("start zookeeper client error:%v", err)
		return err
	}

	g.client.CuratorListenable().AddListener(curator.NewCuratorListener(
		func(client curator.CuratorFramework, event curator.CuratorEvent) error {
			if event.Type() == curator.WATCHED {
				someChange := false

				switch event.WatchedEvent().Type {
				case zk.EventNodeChildrenChanged:
					g.loadNode()
					someChange = true
				case zk.EventNodeDataChanged:
					g.reloadKey(event.Path())
					someChange = true
				default:

				}

				if someChange {
					log.Printf("reload properties with %s", event.Path())
					if g.configLocalCache != nil {
						_, err = g.configLocalCache.saveLocalCache(g, g.node)
						if err != nil {
							log.Printf("save to local file error:%v %v", g.configLocalCache, err)
						}
					}

				}
			}

			return nil
		}))

	err = g.loadNode()
	if err != nil {
		log.Printf("load node error: %v", err)
		return err
	}

	if g.configLocalCache != nil {
		_, err = g.configLocalCache.saveLocalCache(g, g.node)
		if err != nil {
			log.Printf("save to local file error: %v %v", g.configLocalCache, err)
			return err
		}
	}

	// Consistency check
	if g.configProfile.ConsistencyCheck {
		go func() {
			for {
				select {
				case <-time.After(g.configProfile.ConsistencyCheckRate):
					g.loadNode()
				}
			}
		}()
	}

	return nil
}

/**
 * 加载节点并监听节点变化
 */
func (g *ZookeeperConfigGroup) loadNode() error {
	nodePath := MakePath(g.configProfile.versionedRootNode(), g.node)
	childrenBuilder := g.client.GetChildren()
	children, err := childrenBuilder.Watched().ForPath(nodePath)
	if err != nil {
		return err
	}

	configs := make(map[string]string, len(children))
	for _, item := range children {
		key, value, err := g.loadKey(MakePath(nodePath, item))
		if err != nil {
			log.Printf("load property error:%s, %v", item, err)
			return err
		}

		if len(key) > 0 {
			configs[key] = value
		}
	}

	g.PutAll(configs)
	return nil
}

//重新加载某一子节点
func (g *ZookeeperConfigGroup) reloadKey(nodePath string) {
	key, value, _ := g.loadKey(nodePath)
	if len(key) > 0 {
		g.Put(key, value)
	}
}

//加载某一子节点
func (g *ZookeeperConfigGroup) loadKey(nodePath string) (string, string, error) {
	nodeName := getNodeFromPath(nodePath)

	keysSpecified := g.configProfile.KeysSpecified
	switch g.configProfile.KeyLoadingMode {
	case KeyLoadingMode_INCLUDE:
		if keysSpecified == nil || keysSpecified.Contains(nodeName) {
			return ``, ``, nil
		}
	case KeyLoadingMode_EXCLUDE:
		if keysSpecified.Contains(nodeName) {
			return ``, ``, nil
		}
	case KeyLoadingMode_ALL:
	default:
	}

	data := g.client.GetData()
	value, err := data.Watched().ForPath(nodePath)
	if err != nil {
		return ``, ``, err
	}

	return nodeName, string(value), nil
}

/**
 * 导出属性列表
 */
func (g *ZookeeperConfigGroup) exportProperties() map[string]string {
	result := make(map[string]string, g.size())

	g.ForEach(func(key, value string) {
		result[key] = value
	})

	return result
}
