package main

import (
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/config/property"
	"log"
	"math/rand"
	"time"
)

/**
 *
 * This is boilerplate application that configures client to connect Hazelcast Cloud cluster.
 * After successful connection, it puts random entries into the map.
 *
 * See: https://docs.hazelcast.cloud/docs/go-client
 *
 */
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
	cfg.SetProperty(property.StatisticsEnabled.Name(), "true")
	cfg.SetProperty(property.StatisticsPeriodSeconds.Name(), "1")

	client, _ := hazelcast.NewClientWithConfig(cfg)

	mp, _ := client.GetMap("map")
	mp.Put("key", "value")
	val, _ := mp.Get("key")
	if val == "value" {
		log.Println("Connection Successful!")
		log.Println("Now, `map` will be filled with random entries.")
		rand.Seed(time.Now().UTC().UnixNano())
		for true {
			randKey := rand.Intn(100000)
			mp.Put("key"+string(randKey), "value"+string(randKey))
			if randKey%10 == 0 {
				size, _ := mp.Size()
				log.Println(fmt.Sprintf("Map size: %d", size))
			}
		}
	} else {
		panic("Connection failed, check your configuration.")
	}

	client.Shutdown()
}
