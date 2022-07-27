package model

import (
	"encoding/json"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type City struct {
	Country    string `json:"country"`
	City       string `json:"city"`
	Population int64  `json:"population"`
}

func CityAsJson(country string, city string, population int64) serialization.JSON {
	cityObject := &City{Country: country, City: city, Population: population}
	b, err := json.Marshal(cityObject)
	if err != nil {
		panic(err)
	}
	jsonValue := serialization.JSON(b)
	return jsonValue
}
