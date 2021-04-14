package main

import (
	"log"

	"github.com/yxdrlitao/config-toolkit-go"
)

func main() {
	configProfile := config.NewZkConfigProfile(
		`localhost:2181`,
		`/config/demo`,
		"1.0.0",
	)

	zkConfigGroup, err := config.NewZookeeperConfigGroup(
		configProfile,
		`DataSourceGroup`,
	)

	if err != nil {
		panic(err)
	}

	zkConfigGroup.AddWatcher(func(propertyName, value string) {
		log.Printf("config change: %s, %s\n", propertyName, value)
	})

	select {}
}
