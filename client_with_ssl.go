/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/sql"
	"github.com/hazelcast/hazelcast-go-client/types"
	"path/filepath"
	"reflect"
	"time"
)

// A sample application that configures a client to connect to a Hazelcast Viridian cluster
// over TLS, and to then insert and fetch data with SQL, thus testing that the connection to
// the Hazelcast Viridian cluster is successful.
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
	config.Serialization.Compact.SetSerializers(&CitySerializer{})

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
		// don't panic for duplicated keys.
		fmt.Errorf("FAILED. %s", err)
	} else {
		fmt.Print("OK.")
	}

	fmt.Print("\nPutting a city into 'cities' map...")
	cityMap, err := client.GetMap(context.Background(), "cities")
	if err != nil {
		panic(err)
	}

	// Let's also add a city as object.
	city := City{Country: "Brazil", CityName: "Rio de Janeiro", Population: 13634274}
	_, err = cityMap.Put(context.Background(), int32(8), city)
	if err != nil {
		panic(fmt.Errorf("FAILED. %s", err))
	}
	fmt.Print("OK.")
}

func fetchCities(client *hazelcast.Client) {
	fmt.Print("\nFetching cities via SQL...")

	stmt := sql.NewStatement("SELECT __key,this FROM cities")
	result, err := client.SQL().ExecuteStatement(context.Background(), stmt)
	if err != nil {
		panic(fmt.Errorf("querying: %w", err))
	}

	defer result.Close()

	iterator, err := result.Iterator()
	if err != nil {
		panic(fmt.Errorf("acquiring iterator: %w", err))
	}

	fmt.Print("OK.")
	fmt.Println("\n--Results of 'SELECT __key, this FROM cities'")
	fmt.Printf("| %4s | %20s | %20s | %15s |\n", "id", "country", "city", "population")

	for iterator.HasNext() {
		row, err := iterator.Next()
		if err != nil {
			panic(fmt.Errorf("iterating: %w", err))
		}

		id, err := row.GetByColumnName("__key")
		if err != nil {
			panic(fmt.Errorf("getting __key column by name: %w", err))
		}

		c, err := row.GetByColumnName("this")
		if err != nil {
			panic(fmt.Errorf("getting this column by name: %w", err))
		}
		city := c.(City)

		fmt.Printf("| %4d | %20s | %20s | %15d |\n", id, city.Country, city.CityName, city.Population)
	}

	fmt.Println("!! Hint !! You can execute your SQL queries on your Viridian cluster over the management center.")
	fmt.Println(" 1. Go to 'Management Center' of your Hazelcast Viridian cluster. ")
	fmt.Println(" 2. Open the 'SQL Browser'. ")
	fmt.Println(" 3. Try to execute 'SELECT * FROM cities'.")
}

type City struct {
	Country    string
	CityName   string
	Population int32
}

// CitySerializer serializes the City object
type CitySerializer struct{}

func (s CitySerializer) Write(w serialization.CompactWriter, value interface{}) {
	v := value.(City)
	w.WriteString("city", &v.CityName)
	w.WriteString("country", &v.Country)
	w.WriteInt32("population", v.Population)
}

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
