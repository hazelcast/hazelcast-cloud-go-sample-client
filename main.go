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
	cfg.NetworkConfig().SSLConfig().SetEnabled(true)
	cfg.NetworkConfig().SSLConfig().ServerName = "console.hazelcast.cloud"
	discoveryCfg := config.NewCloudConfig()
	discoveryCfg.SetEnabled(true)
	discoveryCfg.SetDiscoveryToken("YOUR_CLUSTER_DISCOVERY_TOKEN")
	cfg.NetworkConfig().SetCloudConfig(discoveryCfg)

	client, _ := hazelcast.NewClientWithConfig(cfg)

	mp, _ := client.GetMap("theGenius")
	mp.Put("vaccination", "Edward Jenner")
	mp.Put("alternating current motor", "Nikola Tesla")
	mp.Put("polonium and radium", "Marie Curie")
	size, _ := mp.Size()
	log.Println("You have " + string(size) + " people in your genius map.")

	client.Shutdown()
}
