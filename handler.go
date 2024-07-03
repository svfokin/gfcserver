package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/spf13/viper"
)

type Config struct {
	Host     string
	Port     string
	Username string
	DBName   string
	SSLMode  string
	Password string
}

type AbonentStr struct {
	Id                string `json:"id"`
	Ls_reg            string `json:"ls_reg"`
	Uuid              string `json:"uuid"`
	Ncounter          string `json:"ncounter"`
	Ls_gas            string `json:"ls_gas"`
	Id_ais            string `json:"id_ais"`
	Database_name     string `json:"database_name"`
	Typecounter       string `json:"typecounter"`
	Street_uuid       string `json:"street_uuid"`
	Fio               string `json:"fio"`
	Adress            string `json:"adress"`
	Id_turg           string `json:"id_turg"`
	Id_rajon          string `json:"id_rajon"`
	Id_filial         string `json:"id_filial"`
	Legal_org         string `json:"legal_org"`
	Verification_date string `json:"verification_date"`
	Ncounter_real     string `json:"ncounter_real"`
	Equipment_uuid    string `json:"equipment_uuid"`
	Working           string `json:"working"`
	Date_remote       string `json:"date_remote"`
	Date_amount       string `json:"date_amount"`
	Amount            string `json:"amount"`
	Equipment_name    string `json:"equipment_name"`
	Department_uuid   string `json:"department_uuid"`
	Update_date       string `json:"update_date"`
}

// upgrader takes an http connection and converts it
// to a websocket one, we're using some recommended
// basic buffer sizes
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func NewPostgresDB(cfg Config) (*sqlx.DB, error) {
	db, err := sqlx.Open("postgres", fmt.Sprintf("host=%s port=%s user=%s dbname=%s password=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.Username, cfg.DBName, cfg.Password, cfg.SSLMode))
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	// upgrade the connection
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("failed to upgrade connection: ", err)
		return
	}
	defer conn.Close()

	//conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	db, err := NewPostgresDB(Config{
		Host:     viper.GetString("db.host"),
		Port:     viper.GetString("db.port"),
		Username: viper.GetString("db.username"),
		DBName:   viper.GetString("db.dbname"),
		SSLMode:  viper.GetString("db.sslmode"),
		Password: os.Getenv("DB_PASSWORD"),
	})

	if err != nil {
		log.Fatalf("failed to initialize db: %s", err.Error())
	}

	kol_ab := 0

	for {
		kol_ab++
		tx := db.MustBegin()

		// read and echo back messages in a loop
		_, p, err := conn.ReadMessage()
		if err != nil {
			log.Println("total received: ", kol_ab-1)
			log.Println("failed to read message: ", err)
			return
		}
		/*
			var msg string

			err = conn.ReadJSON(&msg)
			if err != nil {
				log.Println("failed to read message: ", err)
				return
			}
			var au AbonentStr
			err = json.Unmarshal([]byte(msg), &au)
		*/
		decoded, err := base64.StdEncoding.DecodeString(string(p))
		if err != nil {
			log.Println("failed to decode: ", err)
			return
		}
		//log.Printf("received from client: %s", string(p))
		//log.Printf("received from client: %s", decoded)
		var au AbonentStr
		err = json.Unmarshal(decoded, &au)
		if err != nil {
			log.Println("failed to unmarshal: ", err)
			return
		}
		//err = json.Unmarshal([]byte(p), &au)
		//log.Printf("received from client: %s", au)
		//log.Printf("received from client: %#v", au)
		//log.Println("   ")

		tx.MustExec("INSERT INTO abonents (id, ls_reg, uuid, ncounter, ls_gas, id_ais, database_name, typecounter, fio, adress, id_turg, id_rajon, id_filial, legal_org, verification_date, ncounter_real, equipment_uuid, working, date_remote, date_amount, amount, update_date) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)",
			au.Id, au.Ls_reg, au.Uuid, au.Ncounter, au.Ls_gas, au.Id_ais, au.Database_name, au.Typecounter, au.Fio, au.Adress, au.Id_turg, au.Id_rajon, 0, au.Legal_org, time.Now(), au.Ncounter_real, au.Equipment_uuid, au.Working, time.Now(), time.Now(), 0, au.Update_date)
		/*if err := conn.WriteMessage(messageType, p); err != nil {
			log.Println("failed to write message: ", err)
			return
		}*/
		//conn.SetReadDeadline(time.Now().Add(60 * time.Second))

		if kol_ab%5000 == 0 {
			log.Println("received from client: ", kol_ab)
		}
		tx.Commit()
	}
}
