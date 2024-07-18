package main

import (
	"encoding/base64"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

func main() {

	if err := initConfig(); err != nil {
		log.Fatalf("error initializing configs: %s", err.Error())
	}

	var dbpsswd string = ""

	if err := godotenv.Load(); err != nil {
		log.Fatalf("error loading env variables: %s", err.Error())
	} else {
		var dbps64 = os.Getenv("DB_PASSWORD")
		if len(strings.TrimSpace(dbps64)) != 0 {
			dbpssw, err := base64.StdEncoding.DecodeString(dbps64)
			dbpsswd = string(dbpssw)
			if err != nil {
				log.Fatalf("ERROR decode password: %s", err.Error())
				return
			}
		} else {
			fmt.Println("Введите пароль пользователя базы данных:")
			fmt.Scanf("%s\n", &dbpsswd)
			envstr := "DB_PASSWORD=" + strings.TrimSpace(base64.StdEncoding.EncodeToString([]byte(dbpsswd)))
			env, _ := godotenv.Unmarshal(envstr)
			_ = godotenv.Write(env, "./.env")
		}
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
