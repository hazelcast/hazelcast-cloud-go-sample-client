package main

import (
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/config"
	"log"
)

func main() {

	cfg := hazelcast.NewConfig()
	cfg.GroupConfig().SetName("YOUR_CLUSTER_NAME")
	cfg.GroupConfig().SetPassword("YOUR_CLUSTER_PASSWORD")
	cfg.NetworkConfig().SSLConfig().SetEnabled(false)
	discoveryCfg := config.NewCloudConfig()
	discoveryCfg.SetEnabled(true)
	discoveryCfg.SetDiscoveryToken("YOUR_CLUSTER_DISCOVERY_TOKEN")
	cfg.NetworkConfig().SetCloudConfig(discoveryCfg)
	cfg.SetProperty("hazelcast.client.cloud.url", "YOUR_DISCOVERY_URL")

	client, _ := hazelcast.NewClientWithConfig(cfg)

	mp, _ := client.GetMap("map")
	mp.Put("key", "value")
	size, _ := mp.Size()
	log.Println("You have " + string(size) + " entries in your map.")

	client.Shutdown()
}
