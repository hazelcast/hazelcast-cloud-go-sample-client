package main

import (
	"context"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/types"
	"log"
	"math/rand"
	"os"
	"time"
)

/**
 *
 * This is a boilerplate application that configures the client to connect to your Hazelcast Viridian cluster.
 * After a successful connection, the client puts random entries into the map.
 *
 * See: https://docs.hazelcast.cloud/docs/go-client
 *
 */
func main() {
	// tag::env[]
	// Define which environment to use such as production, uat, or dev
	_ = os.Setenv("HZ_CLOUD_COORDINATOR_BASE_URL", "YOUR_DISCOVERY_URL")
	// end::env[]

	ctx := context.Background()

	// tag::config[]
	// Configure the client to connect to the cluster
	config := hazelcast.NewConfig()
	config.Cluster.Name = "YOUR_CLUSTER_NAME"
	config.Cluster.Network.SSL.Enabled = false
	config.Cluster.Cloud.Enabled = true
	config.Cluster.Cloud.Token = "YOUR_CLUSTER_DISCOVERY_TOKEN"
	config.Stats.Enabled = true
	config.Stats.Period = types.Duration(time.Second)

	client, err := hazelcast.StartNewClientWithConfig(ctx, config)
	if err != nil {
		panic(err)
	}
	// end::config[]

	defer client.Shutdown(ctx)

	log.Println("Connection Successful!")

	log.Println("Now, `map` will be filled with random entries.")

	// Create the map
	mp, err := client.GetMap(ctx, "map")
	if err != nil {
		panic(err)
	}

	// Add random entries to the map
	rand.Seed(time.Now().UTC().UnixNano())
	iterationCounter := 0
	for {
		randKey := string(rune(rand.Intn(100000)))
		_, err := mp.Put(ctx, "key"+randKey, "value"+randKey)
		if err != nil {
			panic(err)
		}
		if iterationCounter++; iterationCounter == 10 {
			iterationCounter = 0
			size, err := mp.Size(ctx)
			if err != nil {
				panic(err)
			}
			log.Println(fmt.Sprintf("Map size: %d", size))
		}
	}
}
