package model

import (
	"encoding/json"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type Country struct {
	IsoCode string `json:"isoCode"`
	Country string `json:"country"`
}

func CountryAsJson(isoCode string, country string) serialization.JSON {
	countryObject := &Country{IsoCode: isoCode, Country: country}
	b, err := json.Marshal(countryObject)
	if err != nil {
		panic(err)
	}
	jsonValue := serialization.JSON(b)
	return jsonValue
}
