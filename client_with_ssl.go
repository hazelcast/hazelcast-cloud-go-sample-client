package main

import (
	"context"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/types"
	"log"
	"math/rand"
	"os"
	"path/filepath"
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
	_ = os.Setenv("HZ_CLOUD_COORDINATOR_BASE_URL", "YOUR_DISCOVERY_URL")
	ctx := context.Background()
	config := hazelcast.NewConfig()
	config.Cluster.Name = "YOUR_CLUSTER_NAME"
	config.Cluster.Cloud.Enabled = true
	config.Cluster.Cloud.Token = "YOUR_CLUSTER_DISCOVERY_TOKEN"
	config.Stats.Enabled = true
	config.Stats.Period = types.Duration(time.Second)
	config.Logger.Level = "debug"
	caFile, err := filepath.Abs("./ca.pem")
	if err != nil {
		panic(err)
	}
	certFile, err := filepath.Abs("./cert.pem")
	if err != nil {
		panic(err)
	}
	keyFile, err := filepath.Abs("./key.pem")
	if err != nil {
		panic(err)
	}
	config.Cluster.Network.SSL.Enabled = true
	err = config.Cluster.Network.SSL.SetCAPath(caFile)
	if err != nil {
		panic(err)
	}
	err = config.Cluster.Network.SSL.AddClientCertAndEncryptedKeyPath(certFile, keyFile, "YOUR_SSL_PASSWORD")
	if err != nil {
		panic(err)
	}
	client, err := hazelcast.StartNewClientWithConfig(ctx, config)
	if err != nil {
		panic(err)
	}
	defer client.Shutdown(ctx)

	log.Println("Connection Successful!")

	log.Println("Now, `map` will be filled with random entries.")

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
			log.Println(fmt.Sprintf("Map size: %d", size))
		}
	}

}
