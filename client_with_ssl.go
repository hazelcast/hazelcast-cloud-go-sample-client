package main

import (
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/config"
	"log"
	"os"
	"io/ioutil"
)

func main() {

	cfg := hazelcast.NewConfig()
	sslConfig := cfg.NetworkConfig().SSLConfig()
  sslConfig.SetEnabled(true)
	caFile,err := filepath.Abs("./ca.pem")
	certFile,err := filepath.Abs("./cert.pem")
	keyFile,err := filepath.Abs("./key.pem")
  sslConfig.SetCaPath(caFile)
  sslConfig.AddClientCertAndEncryptedKeyPath(certFile, keyFile, "YOUR_SSL_PASSWORD")
	cfg.GroupConfig().SetName("YOUR_CLUSTER_NAME")
	cfg.GroupConfig().SetPassword("YOUR_CLUSTER_PASSWORD")
	discoveryCfg := config.NewCloudConfig()
	discoveryCfg.SetEnabled(true)
	discoveryCfg.SetDiscoveryToken("YOUR_CLUSTER_DISCOVERY_TOKEN")
	cfg.NetworkConfig().SetCloudConfig(discoveryCfg)
	cfg.SetProperty("hazelcast.client.cloud.url", "YOUR_DISCOVERY_URL")

	client, _ := hazelcast.NewClientWithConfig(cfg)

	mp, _ := client.GetMap("map")
	mp.Put("key", "value")
	value, _ := mp.Get("key")
	log.Println("Value:", value)
	client.Shutdown()
}
