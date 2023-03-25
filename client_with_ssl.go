package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
	"path/filepath"
	"reflect"
	"time"
)

// This is boilerplate application that configures client to connect Hazelcast Cloud cluster.
// After successful connection, it runs the uncommented examples.
//
// See: https://docs.hazelcast.com/cloud/get-started
func main() {
	ctx := context.Background()
	config := hazelcast.NewConfig()
	config.Cluster.Name = "YOUR_CLUSTER_NAME"
	config.Cluster.Cloud.Enabled = true
	config.Cluster.Cloud.Token = "YOUR_CLUSTER_DISCOVERY_TOKEN"
	config.Stats.Enabled = true
	config.Stats.Period = types.Duration(time.Second)
	
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
	defer client.Shutdown(ctx)
	fmt.Println("Connection Successful!")

	createMapping(client)
	populateCities(client)
	fetchCities(client)
}

func createMapping(client *hazelcast.Client) {
	// See: https://docs.hazelcast.com/hazelcast/latest/sql/mapping-to-maps
	fmt.Print("\nCreating the mapping...")
	mappingQuery := fmt.Sprintf(`
		 CREATE OR REPLACE MAPPING 
		 cities (
			__key INT,                                        
			country VARCHAR,
			city VARCHAR,
			population INT) TYPE IMAP
		 OPTIONS ( 
			'keyFormat' = 'int',
			'valueFormat' = 'compact',
			'valueCompactTypeName' = 'city')`)
	_, err := client.SQL().Execute(context.Background(), mappingQuery)
	if err != nil {
		panic(err)
	}
	fmt.Print("OK.")
}

func populateCities(client *hazelcast.Client) {
	ctx := context.Background()
	fmt.Print("\nInserting data via SQL...")
	insertQuery := fmt.Sprintf(`INSERT INTO cities 
										(__key, city, country, population) VALUES
										(1, 'London', 'United Kingdom', 9540576),
										(2, 'Manchester', 'United Kingdom', 2770434),
										(3, 'New York', 'United States', 19223191),
										(4, 'Los Angeles', 'United States', 3985520),
										(5, 'Istanbul', 'Türkiye', 15636243),
										(6, 'Ankara', 'Türkiye', 5309690),
										(7, 'Sao Paulo ', 'Brazil', 22429800)`)

	_, err := client.SQL().Execute(context.Background(), insertQuery)
	if err != nil {
		fmt.Print("FAILED. ", err)
	} else {
		fmt.Print("OK.")
	}

	fmt.Print("\nPutting a city into 'cities' map...")
	cityMap, err := client.GetMap(ctx, "cities")
	if err != nil {
		panic(err)
	}

	city := City{Country: "Brazil", CityName: "Rio de Janeiro", Population: 13634274}
	cityMap.Put(ctx, 8, city)
	fmt.Print("OK.")
}

func fetchCities(client *hazelcast.Client) {
	fmt.Print("\nFetching cities via SQL...")
	ctx := context.Background()

	result, err := client.SQL().Execute(ctx, "SELECT __key,this FROM cities")
	if err != nil {
		fmt.Errorf("querying: %w", err)
	}

	defer result.Close()

	iterator, err := result.Iterator()
	if err != nil {
		fmt.Errorf("acquiring iterator: %w", err)
	}

	fmt.Print("OK.")
	fmt.Println("\n--Results of 'SELECT __key, this FROM cities'")
	fmt.Printf("| %4s | %20s | %20s | %15s |\n", "id", "country", "city", "population")

	for iterator.HasNext() {
		row, err := iterator.Next()
		if err != nil {
			fmt.Errorf("iterating: %w", err)
		}

		id := mustGet(row.GetByColumnName("__key"))
		city := mustGet(row.GetByColumnName("this")).(City)

		fmt.Printf("| %4s | %20s | %20s | %15d |", id, city.Country, city.CityName, city.Population)
	}
}

func mustGet(value interface{}, err error) interface{} {
	if err != nil {
		panic(err)
	}
	return value
}

type City struct {
	Country    string
	CityName   string
	Population int32
}

// CitySerializer serializes the City object
type CitySerializer struct{}

func (s CitySerializer) Type() reflect.Type {
	return reflect.TypeOf(City{})
}

func (s CitySerializer) TypeName() string {
	return "city"
}

func (s CitySerializer) Read(r serialization.CompactReader) interface{} {
	var cityName string
	var country string
	var population int32

	n := r.ReadString("city")
	if n != nil {
		cityName = *n
	}

	n = r.ReadString("country")
	if n != nil {
		country = *n
	}

	population = r.ReadInt32("population")

	return City{Country: country, CityName: cityName, Population: population}
}
