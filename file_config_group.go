package config

import (
	"fmt"

	"github.com/go-xweb/log"
)

type FileConfigGroup struct {
	configProfile *FileConfigProfile
	location      *FileLocation
	protocol      Protocol
	*GeneralConfigGroup
}

func NewFileConfigGroup(internalConfigGroup ConfigGroup, configProfile *FileConfigProfile, location string) (*FileConfigGroup, error) {
	group := &FileConfigGroup{
		configProfile:      configProfile,
		location:           newFileLocation(location),
		GeneralConfigGroup: NewGeneralConfigGroup(internalConfigGroup),
	}

	if err := group.initConfig(); err != nil {
		return group, err
	}

	return group, nil
}

func (g *FileConfigGroup) initConfig() error {
	protocol := g.location.selectProtocol()
	if protocol == nil {
		return fmt.Errorf("can't resolve protocol:%v", g.location)
	}

	g.protocol = protocol

	contentTypeResolve, err := selectContentTypeResolve(g.configProfile.contentType)
	if err != nil {
		log.Printf("fileConfigGroup init failed :%v, contentType%s", err, g.configProfile.contentType)
		return err
	}

	data, err := g.protocol.Read(g.location)
	if err != nil {
		log.Printf("fileConfigGroup init failed :%v, read file error:%s\n", err, g.location.file)
		return err
	}

	properties, err := contentTypeResolve.resolve(data, g.location.protocol)
	if err != nil {
		log.Printf("fileConfigGroup init failed :%v, contentType%s", err, g.configProfile.contentType)
		return err
	}

	for key, value := range properties {
		g.Put(key, value)
	}

	return nil
}
