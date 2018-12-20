package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	hazelcast "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/config"
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
            time.Sleep(100 * time.Millisecond)
        }
    } else {
        panic("Connection failed, check your configuration.")
    }

    client.Shutdown()
}