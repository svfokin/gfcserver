package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

func main() {
	if err := initConfig(); err != nil {
		log.Fatalf("error initializing configs: %s", err.Error())
	}

	if err := godotenv.Load(); err != nil {
		log.Fatalf("error loading env variables: %s", err.Error())
	}

	ip := viper.GetString("ws.ip")
	portIP := viper.GetString("ws.port_ip")

	fmt.Println("Listening on port :" + portIP)
	// we mount our single handler on port localhost:8000 to handle all
	// requests
	log.Panic(http.ListenAndServe(ip+":"+portIP, http.HandlerFunc(wsHandler)))
}

func initConfig() error {
	viper.AddConfigPath("configs")
	viper.SetConfigName("config")
	return viper.ReadInConfig()
}
