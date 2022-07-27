package main

import (
	"context"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/sql"
	"github.com/hazelcast/hazelcast-go-client/types"
	"local/build/model"

	"log"
	"math/rand"
	"os"
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
	config.Cluster.Network.SSL.Enabled = false
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

	mapExample(client)

	//sqlExample(client)

	//jsonSerializationExample(client)

	//nonStopMapExample(client)

}

/**
 * This example shows how to work with Hazelcast maps.
 *
 * @param client - a {@link hazelcast.Client} client.
 */
func mapExample(client *hazelcast.Client) {
	ctx := context.Background()
	cities, err := client.GetMap(ctx, "cities")
	if err != nil {
		panic(err)
	}

	cities.Put(ctx, "1", model.CityAsJson("United Kingdom", "London", 9_540_576))
	cities.Put(ctx, "2", model.CityAsJson("United Kingdom", "Manchester", 2_770_434))
	cities.Put(ctx, "3", model.CityAsJson("United States", "New York", 19_223_191))
	cities.Put(ctx, "4", model.CityAsJson("United States", "Los Angeles", 3_985_520))
	cities.Put(ctx, "5", model.CityAsJson("Turkey", "Ankara", 5_309_690))
	cities.Put(ctx, "6", model.CityAsJson("Turkey", "Istanbul", 15_636_243))
	cities.Put(ctx, "7", model.CityAsJson("Brazil", "Sao Paulo", 22_429_800))
	cities.Put(ctx, "8", model.CityAsJson("Brazil", "Rio de Janeiro", 13_634_274))

	mapSize, err := cities.Size(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Printf("'cities' map now contains %d entries.\n", mapSize)
	fmt.Println("--------------------")

}

/**
 * This example shows how to work with Hazelcast SQL queries.
 *
 * @param client - a {@link hazelcast.Client} client.
 */
func sqlExample(client *hazelcast.Client) {
	sqlService := client.SQL()

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
		)
	`)

	ignored, err := sqlService.Execute(context.Background(), mappingQuery)
	defer ignored.Close()

	if err != nil {
		panic(err)
	}
	fmt.Println("The mapping has been created successfully.")
	fmt.Println("--------------------")

}

func clearCapitals(sqlService sql.Service) {
	fmt.Println("Deleting data via SQL...")

	ignored, err := sqlService.Execute(context.Background(), "DELETE FROM capitals")
	defer ignored.Close()
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

	ignored, err := sqlService.Execute(context.Background(), insertQuery)
	defer ignored.Close()
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
		fmt.Errorf("Querying: %w", err)
	}

	iterator, err := result.Iterator()
	if err != nil {
		fmt.Errorf("Acquaring iterator: %w", err)
	}

	for iterator.HasNext() {
		row, err := iterator.Next()
		if err != nil {
			panic(err)
		}
		country, err := row.Get(0)
		city, err := row.Get(1)

		fmt.Printf("%s - %s\n", country, city)
	}
	fmt.Println("--------------------")
}

func selectCapitalNames(sqlService sql.Service) {
	ctx := context.Background()
	fmt.Println("Retrieving the capital name via SQL...")
	result, err := sqlService.Execute(ctx, "SELECT __key, this FROM capitals WHERE __key = ?", "United States")

	iter, err := result.Iterator()
	if err != nil {
		fmt.Errorf("acquaring iterator: %w", err)
	}
	for iter.HasNext() {

		row, err := iter.Next()
		if err == nil {
			fmt.Errorf("iterating: %w", err)
		}
		country, err := row.GetByColumnName("__key")
		city, err := row.GetByColumnName("this")

		fmt.Printf("%s - %s\n", country, city)
	}

	fmt.Println("--------------------")
}

/**
 * This example shows how to work with Hazelcast SQL queries via Maps that
 * contains JSON serialized values.
 *
 * <ul>
 *     <li>Select single json element data from a Map</li>
 *     <li>Select data from Map with filtering</li>
 *     <li>Join data from two Maps and select json elements</li>
 * </ul>
 *
 * @param client - a {@link hazelcast.Client} client.
 */
func jsonSerializationExample(client *hazelcast.Client) {

	sqlService := client.SQL()

	createMappingForCountries(sqlService)

	populateCountriesWithMap(client)

	selectAllCountries(sqlService)

	createMappingForCities(sqlService)

	populateCities(client)

	selectCitiesByCountry(sqlService, "US")

	selectCountriesAndCities(sqlService)

}

func createMappingForCountries(sqlService sql.Service) {
	//see: https://docs.hazelcast.com/hazelcast/5.1/sql/mapping-to-maps#json-objects
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
        )
	`)
	ignored, err := sqlService.Execute(context.Background(), mappingQuery)
	defer ignored.Close()

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

	countries.Put(ctx, "AU", model.CountryAsJson("AU", "Australia"))
	countries.Put(ctx, "EN", model.CountryAsJson("EN", "England"))
	countries.Put(ctx, "US", model.CountryAsJson("US", "United States"))
	countries.Put(ctx, "CZ", model.CountryAsJson("CZ", "Czech Republic"))

	fmt.Println("The 'countries' map has been populated.")
	fmt.Println("--------------------")

}

func selectAllCountries(sqlService sql.Service) {
	ctx := context.Background()
	sql := "SELECT c.country from country c"
	fmt.Println("Select all countries with sql = " + sql)

	result, err := sqlService.Execute(ctx, sql)
	if err != nil {
		panic(err)
	}

	iter, err := result.Iterator()
	if err != nil {
		panic(err)
	}

	for iter.HasNext() {
		row, err := iter.Next()
		if err != nil {
			panic(err)
		}
		country, err := row.GetByColumnName("country")
		fmt.Printf("Country = %s\n", country)
	}
	fmt.Println("--------------------")
}

func createMappingForCities(sqlService sql.Service) {
	//see: https://docs.hazelcast.com/hazelcast/5.1/sql/mapping-to-maps#json-objects
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
        )
	`)

	result, err := sqlService.Execute(context.Background(), mappingSql)
	defer result.Close()
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

	cities.Put(ctx, 1, model.CityAsJson("AU", "Canberra", 467_194))
	cities.Put(ctx, 2, model.CityAsJson("CZ", "Prague", 1_318_085))
	cities.Put(ctx, 3, model.CityAsJson("EN", "London", 9_540_576))
	cities.Put(ctx, 4, model.CityAsJson("US", "Washington, DC", 7_887_965))

	fmt.Println("The 'City' map has been populated.")
	fmt.Println("--------------------")

}

func selectCitiesByCountry(sqlService sql.Service, country string) {
	sql := "SELECT city, population FROM city WHERE country=?"
	fmt.Println("Select city and population with sql = " + sql)

	result, err := sqlService.Execute(context.Background(), sql, country)
	if err != nil {
		panic(err)
	}

	iter, err := result.Iterator()
	if err != nil {
		panic(err)
	}

	for iter.HasNext() {
		row, err := iter.Next()
		if err != nil {
			panic(err)
		}
		country, err := row.GetByColumnName("city")
		population, err := row.GetByColumnName("population")
		fmt.Printf("City = %s, Population = %d\n", country, population)
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
		panic(err)
	}

	iter, err := result.Iterator()
	if err != nil {
		panic(err)
	}

	for iter.HasNext() {

		row, err := iter.Next()
		if err != nil {

			panic(err)
		}

		isoCode, err := row.GetByColumnName("isoCode")
		country, err := row.GetByColumnName("country")
		city, err := row.GetByColumnName("city")
		population, err := row.GetByColumnName("population")

		fmt.Printf("%4s | %15s | %20s | %15d |\n", isoCode, country, city, population)
	}

	fmt.Println("--------------------")

}

/**
 * This example shows how to work with Hazelcast maps, where the map is
 * updated continuously.
 *
 * @param client - a {@link hazelcast.Client} client.
 */
func nonStopMapExample(client *hazelcast.Client) {
	fmt.Println("Now the map named 'map' will be filled with random entries.")
	ctx := context.Background()

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
