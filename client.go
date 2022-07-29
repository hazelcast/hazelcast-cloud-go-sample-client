package main

import (
	"context"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/sql"
	"github.com/hazelcast/hazelcast-go-client/types"
	"log"
	"math/rand"
	"time"
)

// This is boilerplate application that configures client to connect Hazelcast Cloud cluster.
// After successful connection, it runs the uncommented examples.
//
// See: https://docs.hazelcast.com/cloud/go-client
func main() {
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
	// sqlExample(client)
	// jsonSerializationExample(client)
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

func country(isoCode string, name string) serialization.JSON {
	text := fmt.Sprintf(`{
		"isoCode":"%s", 
		"country": "%s"
	}`, isoCode, name)
	return serialization.JSON(text)
}

func mustPut(_ interface{}, err error) {
	if err != nil {
		panic(err)
	}
}

func mustGet(value interface{}, err error) interface{} {
	if err != nil {
		panic(err)
	}
	return value
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

// sqlExample shows how to work with Hazelcast SQL queries.
// It maps the "capitals" map to a database table and runs queries on it.
func sqlExample(client *hazelcast.Client) {
	sqlService := client.SQL()
	// Separate examples to show the SQL support of Hazelcast Go Client
	createMappingForCapitals(sqlService)
	clearCapitals(sqlService)
	populateCapitals(sqlService)
	selectAllCapitals(sqlService)
	selectCapitalNames(sqlService)
}

func createMappingForCapitals(sqlService sql.Service) {
	// See: https://docs.hazelcast.com/hazelcast/5.1/sql/mapping-to-maps
	fmt.Println("Creating a mapping...")
	mappingQuery := fmt.Sprintf(`
        CREATE OR REPLACE MAPPING capitals TYPE IMap
		OPTIONS (
			'keyFormat' = 'varchar',
			'valueFormat' = 'varchar'
	)`)
	_, err := sqlService.Execute(context.Background(), mappingQuery)
	if err != nil {
		panic(err)
	}
	fmt.Println("The mapping has been created successfully.")
	fmt.Println("--------------------")
}

func clearCapitals(sqlService sql.Service) {
	fmt.Println("Deleting data via SQL...")
	_, err := sqlService.Execute(context.Background(), "DELETE FROM capitals")
	if err != nil {
		panic(err)
	}
	fmt.Println("The data has been deleted successfully.")
	fmt.Println("--------------------")
}

func populateCapitals(sqlService sql.Service) {
	fmt.Println("Inserting data via SQL...")
	insertQuery := fmt.Sprintf(`
        INSERT INTO capitals VALUES
			('Australia','Canberra'),
			('Croatia','Zagreb'),
			('Czech Republic','Prague'),
			('England','London'),
			('Turkey','Ankara'),
			('United States','Washington, DC');
	`)
	_, err := sqlService.Execute(context.Background(), insertQuery)
	if err != nil {
		panic(err)
	}
	fmt.Println("The data has been inserted successfully.")
	fmt.Println("--------------------")
}

func selectAllCapitals(sqlService sql.Service) {
	fmt.Println("Retrieving all the data via SQL...")
	ctx := context.Background()
	result, err := sqlService.Execute(ctx, "SELECT * FROM capitals")
	if err != nil {
		fmt.Errorf("querying: %w", err)
	}
	defer result.Close()
	iterator, err := result.Iterator()
	if err != nil {
		fmt.Errorf("acquiring iterator: %w", err)
	}
	for iterator.HasNext() {
		row, err := iterator.Next()
		if err != nil {
			fmt.Errorf("iterating: %w", err)
		}
		country := mustGet(row.Get(0))
		city := mustGet(row.Get(1))
		fmt.Printf("%s - %s\n", country, city)
	}
	fmt.Println("--------------------")
}

func selectCapitalNames(sqlService sql.Service) {
	ctx := context.Background()
	fmt.Println("Retrieving the capital name via SQL...")
	result, err := sqlService.Execute(ctx, "SELECT __key, this FROM capitals WHERE __key = ?", "United States")
	if err != nil {
		fmt.Errorf("querying: %w", err)
	}
	defer result.Close()
	iter, err := result.Iterator()
	if err != nil {
		fmt.Errorf("acquiring iterator: %w", err)
	}
	for iter.HasNext() {
		row, err := iter.Next()
		if err != nil {
			fmt.Errorf("iterating: %w", err)
		}
		country := mustGet(row.GetByColumnName("__key"))
		city := mustGet(row.GetByColumnName("this"))
		fmt.Printf("Country name: %s; Capital name: %s\n", country, city)
	}
	fmt.Println("--------------------")
}

// jsonSerializationExample shows how to work with Hazelcast SQL queries via Maps that
// contains JSON serialized values.
//
// (1) Select single json element data from a Map.
// (2) Select data from Map with filtering.
// (3) Join data from two Maps and select json elements.
func jsonSerializationExample(client *hazelcast.Client) {
	sqlService := client.SQL()
	// Separate examples to show JSON type support of Hazelcast Go Client
	createMappingForCountries(sqlService)
	populateCountriesWithMap(client)
	selectAllCountries(sqlService)
	createMappingForCities(sqlService)
	populateCities(client)
	selectCitiesByCountry(sqlService, "AU")
	selectCountriesAndCities(sqlService)
}

func createMappingForCountries(sqlService sql.Service) {
	// see: https://docs.hazelcast.com/hazelcast/5.1/sql/mapping-to-maps#json-objects
	fmt.Println("Creating mapping for countries...")
	mappingQuery := fmt.Sprintf(`
        CREATE OR REPLACE MAPPING country (
			__key VARCHAR,
			isoCode VARCHAR,
			country VARCHAR
		)
        TYPE IMAP 
        OPTIONS (
            'keyFormat' = 'varchar',
            'valueFormat' = 'json-flat'
		)`)
	_, err := sqlService.Execute(context.Background(), mappingQuery)
	if err != nil {
		panic(err)
	}
	fmt.Println("Mapping for countries has been created.")
	fmt.Println("--------------------")
}

func populateCountriesWithMap(client *hazelcast.Client) {
	// see: https://docs.hazelcast.com/hazelcast/5.1/data-structures/creating-a-map#writing-json-to-a-map
	ctx := context.Background()
	fmt.Println("Populating 'countries' map with JSON values...")
	countries, err := client.GetMap(ctx, "country")
	if err != nil {
		panic(err)
	}
	mustPut(countries.Put(ctx, "AU", country("AU", "Australia")))
	mustPut(countries.Put(ctx, "EN", country("EN", "England")))
	mustPut(countries.Put(ctx, "US", country("US", "United States")))
	mustPut(countries.Put(ctx, "CZ", country("CZ", "Czech Republic")))
	fmt.Println("The 'countries' map has been populated.")
	fmt.Println("--------------------")
}

func selectAllCountries(sqlService sql.Service) {
	ctx := context.Background()
	sql := "SELECT c.country from country c"
	fmt.Println("Select all countries with sql = " + sql)
	result, err := sqlService.Execute(ctx, sql)
	if err != nil {
		fmt.Errorf("querying: %w", err)
	}
	defer result.Close()
	iter, err := result.Iterator()
	if err != nil {
		fmt.Errorf("acquiring iterator: %w", err)
	}
	for iter.HasNext() {
		row, err := iter.Next()
		if err != nil {
			fmt.Errorf("iterating: %w", err)
		}
		country := mustGet(row.GetByColumnName("country"))
		fmt.Printf("Country = %s\n", country)
	}
	fmt.Println("--------------------")
}

func createMappingForCities(sqlService sql.Service) {
	// see: https://docs.hazelcast.com/hazelcast/5.1/sql/mapping-to-maps#json-objects
	fmt.Println("Creating mapping for cities...")
	mappingSql := fmt.Sprintf(`
        CREATE OR REPLACE MAPPING city (
			__key INT,
			country VARCHAR,
			city VARCHAR,
			population BIGINT)
        TYPE IMAP 
        OPTIONS (
            'keyFormat' = 'int',
            'valueFormat' = 'json-flat'
        )`)
	_, err := sqlService.Execute(context.Background(), mappingSql)
	if err != nil {
		panic(err)
	}
	fmt.Println("Mapping for cities has been created")
	fmt.Println("--------------------")
}

func populateCities(client *hazelcast.Client) {
	// see: https://docs.hazelcast.com/hazelcast/5.1/data-structures/creating-a-map#writing-json-to-a-map
	fmt.Println("Populating 'city' map with JSON values...")
	ctx := context.Background()
	cities, err := client.GetMap(ctx, "city")
	if err != nil {
		panic(err)
	}
	mustPut(cities.Put(ctx, 1, city("AU", "Canberra", 467_194)))
	mustPut(cities.Put(ctx, 2, city("CZ", "Prague", 1_318_085)))
	mustPut(cities.Put(ctx, 3, city("EN", "London", 9_540_576)))
	mustPut(cities.Put(ctx, 4, city("US", "Washington, DC", 7_887_965)))
	fmt.Println("The 'city' map has been populated.")
	fmt.Println("--------------------")
}

func selectCitiesByCountry(sqlService sql.Service, country string) {
	sql := "SELECT city, population FROM city WHERE country=?"
	fmt.Println("Select city and population with sql = " + sql)
	result, err := sqlService.Execute(context.Background(), sql, country)
	if err != nil {
		fmt.Errorf("querying: %w", err)
	}
	defer result.Close()
	iter, err := result.Iterator()
	if err != nil {
		fmt.Errorf("acquiring iterator: %w", err)
	}
	for iter.HasNext() {
		row, err := iter.Next()
		if err != nil {
			fmt.Errorf("iterating: %w", err)
		}
		city := mustGet(row.GetByColumnName("city"))
		population := mustGet(row.GetByColumnName("population"))
		fmt.Printf("City = %s, Population = %d\n", city, population)
	}
	fmt.Println("--------------------")
}

func selectCountriesAndCities(sqlService sql.Service) {
	query := fmt.Sprintf(`
        SELECT c.isoCode, c.country, t.city, t.population
		  FROM country c
		       JOIN city t ON c.isoCode = t.country
	`)
	fmt.Println("Select country and city data in query that joins tables")
	fmt.Printf("%4s | %15s | %20s | %15s |\n", "iso", "country", "city", "population")
	result, err := sqlService.Execute(context.Background(), query)
	if err != nil {
		fmt.Errorf("querying: %w", err)
	}
	defer result.Close()
	iter, err := result.Iterator()
	if err != nil {
		fmt.Errorf("acquiring iterator: %w", err)
	}
	for iter.HasNext() {
		row, err := iter.Next()
		if err != nil {
			fmt.Errorf("iterating: %w", err)
		}
		isoCode := mustGet(row.GetByColumnName("isoCode"))
		country := mustGet(row.GetByColumnName("country"))
		city := mustGet(row.GetByColumnName("city"))
		population := mustGet(row.GetByColumnName("population"))
		fmt.Printf("%4s | %15s | %20s | %15d |\n", isoCode, country, city, population)
	}
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
