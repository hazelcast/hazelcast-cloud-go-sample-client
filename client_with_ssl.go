package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/types"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

/*
 * This is a boilerplate client application that connects to your Hazelcast Viridian cluster.
 * See: https://docs.hazelcast.com/cloud/get-started
 * 
 * Snippets of this code are included as examples in our documentation,
 * using the tag:: comments.
*/
func main() {
	// tag::env[]
	// Define which environment to use such as production, uat, or dev
	_ = os.Setenv("HZ_CLOUD_COORDINATOR_BASE_URL", "YOUR_DISCOVERY_URL")
	// end::env[]

	ctx := context.Background()

	// Configure the client to connect to the cluster
	// tag::config[]
	config := hazelcast.NewConfig()
	config.Cluster.Name = "YOUR_CLUSTER_NAME"
	config.Cluster.Cloud.Enabled = true
	/* The cluster discovery token is a unique token that maps to the current IP address of the cluster.
		Cluster IP addresses may change.
		This token allows clients to find out the current IP address
		of the cluster and connect to it.
	*/
	config.Cluster.Cloud.Token = "YOUR_CLUSTER_DISCOVERY_TOKEN"
	/* Allow the client to collect metrics
	 * so that you can see client statistics in Management Center.
	 * See https://pkg.go.dev/github.com/hazelcast/hazelcast-go-client#hdr-Management_Center_Integration
	*/
	config.Stats.Enabled = true
	config.Stats.Period = types.Duration(time.Second)
	// Configure TLS
	// When you download the sample client from the console,
	// the files are downloaded along with this client code.
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
	config.Cluster.Network.SSL.SetTLSConfig(&tls.Config{ServerName: "hazelcast.cloud"})
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
