package main

import (
	"context"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
	"log"
	"math/rand"
	"os"
	"time"
)

// This is boilerplate application that configures client to connect Hazelcast Cloud cluster.
// After successful connection, it runs the uncommented examples.
//
// See: https://docs.hazelcast.com/cloud/go-client
func main() {
	_ = os.Setenv("HZ_CLOUD_COORDINATOR_BASE_URL", "YOUR_DISCOVERY_URL")
	ctx := context.Background()
	config := hazelcast.NewConfig()
	config.Cluster.Name = "YOUR_CLUSTER_NAME"
	config.Cluster.Cloud.Enabled = true
	config.Cluster.Cloud.Token = "YOUR_CLUSTER_DISCOVERY_TOKEN"
	config.Stats.Enabled = true
	config.Stats.Period = types.Duration(time.Second)
	client, err := hazelcast.StartNewClientWithConfig(ctx, config)
	if err != nil {
		panic(err)
	}
	defer client.Shutdown(ctx)
	fmt.Println("Connection Successful!")

	// Uncomment the example you want to run.
	mapExample(client)
	// nonStopMapExample(client)
}

func city(country string, name string, population int) serialization.JSON {
	text := fmt.Sprintf(`{
		"country":"%s", 
		"city": "%s", 
		"population":"%d"
	}`, country, name, population)
	return serialization.JSON(text)

}

func mustPut(_ interface{}, err error) {
	if err != nil {
		panic(err)
	}
}

// mapExample shows how to work with Hazelcast maps.
// It simply puts a bunch of entries to the map.
func mapExample(client *hazelcast.Client) {
	ctx := context.Background()
	cities, err := client.GetMap(ctx, "cities")
	if err != nil {
		panic(err)
	}
	mustPut(cities.Put(ctx, "1", city("United Kingdom", "London", 9_540_576)))
	mustPut(cities.Put(ctx, "2", city("United Kingdom", "Manchester", 2_770_434)))
	mustPut(cities.Put(ctx, "3", city("United States", "New York", 19_223_191)))
	mustPut(cities.Put(ctx, "4", city("United States", "Los Angeles", 3_985_520)))
	mustPut(cities.Put(ctx, "5", city("Turkey", "Ankara", 5_309_690)))
	mustPut(cities.Put(ctx, "6", city("Turkey", "Istanbul", 15_636_243)))
	mustPut(cities.Put(ctx, "7", city("Brazil", "Sao Paulo", 22_429_800)))
	mustPut(cities.Put(ctx, "8", city("Brazil", "Rio de Janeiro", 13_634_274)))
	mapSize, err := cities.Size(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Printf("'cities' map now contains %d entries.\n", mapSize)
	fmt.Println("--------------------")
}

// nonStopMapExample shows how to work with Hazelcast maps, where the map is
// updated continuously.
func nonStopMapExample(client *hazelcast.Client) {
	fmt.Println("Now the map named 'map' will be filled with random entries.")
	ctx := context.TODO()
	mp, err := client.GetMap(ctx, "map")
	if err != nil {
		panic(err)
	}
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
			log.Println(fmt.Sprintf("Current map size: %d", size))
		}
	}
}
