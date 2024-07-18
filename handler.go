package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
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

// Функция проверяет наличие папки log в текущим каталоге и создает её при отсутствии
// Возвращает полный путь
func DirExist() string {
	var here = os.Args[0]
	here1 := filepath.Dir(here)
	/*if err != nil {
		fmt.Printf("Неправильный путь: %s\n", err)
	}*/
	dir := here1 + string(os.PathSeparator) + "log"
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err := os.Mkdir(dir, 0755)
		if err != nil {
			return here
		}
	}
	return dir
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

func readPsswd() string {
	var dbpass string = ""
	var dbps64 = os.Getenv("DB_PASSWORD")
	if len(strings.TrimSpace(dbps64)) != 0 {
		dbpssw, err := base64.StdEncoding.DecodeString(dbps64)
		dbpass = string(dbpssw)
		if err != nil {
			log.Fatalf("ERROR decode password: %s", err.Error())
			return ""
		}
	}
	return dbpass
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	// обновляем соединение
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("failed to upgrade connection: ", err)
		return
	}
	defer conn.Close()

	db, err := NewPostgresDB(Config{
		Host:     viper.GetString("db.host"),
		Port:     viper.GetString("db.port"),
		Username: viper.GetString("db.username"),
		DBName:   viper.GetString("db.dbname"),
		SSLMode:  viper.GetString("db.sslmode"),
		Password: readPsswd(),
	})

	if err != nil {
		log.Fatalf("failed to initialize db: %s", err.Error())
	}

	// текущие дата время для формирования имени log-файла
	t := time.Now()
	LOGFILE := path.Join(DirExist(), t.Format("20060102150405")+".log")
	f, err := os.OpenFile(LOGFILE, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer f.Close()

	Info := log.New(f, "", log.LstdFlags)
	Info.Println("New import session")
	Info.SetFlags(log.Ltime)

	var tr bool = true
	var tx *sqlx.Tx
	var query string
	values := []interface{}{}

	numFields := 22 // the number of fields you are inserting
	kol_ab := 0
	kol_tr := 0

	for {
		kol_ab++
		kol_tr++

		// читаем сообщения с цикле
		_, p, err := conn.ReadMessage()
		if err != nil {
			log.Println("total received: ", kol_ab-1)
			log.Println("failed to read message: ", err)
			Info.SetFlags(log.LstdFlags)
			Info.Println("End of import. All: " + strconv.Itoa(kol_ab-1))
			if !tr {
				tx.MustExec(query, values...)
				tx.Commit()
			}
			return
		}

		if tr {
			tx = db.MustBegin()
			query = "INSERT INTO abonents (id, ls_reg, uuid, ncounter, ls_gas, id_ais, database_name, typecounter, fio, adress, id_turg, id_rajon, id_filial, legal_org, verification_date, ncounter_real, equipment_uuid, working, date_remote, date_amount, amount, update_date) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)"
			//очищаем срез интерфейсов
			values = values[:0]
			tr = false
			kol_tr = 0
		}

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

		Info.Println(strconv.Itoa(kol_ab) + " " + au.Equipment_uuid)
		values = append(values, au.Id, au.Ls_reg, au.Uuid, au.Ncounter, au.Ls_gas, au.Id_ais, au.Database_name, au.Typecounter, au.Fio, au.Adress, au.Id_turg, au.Id_rajon, 0, au.Legal_org, time.Now(), au.Ncounter_real, au.Equipment_uuid, au.Working, time.Now(), time.Now(), 0, au.Update_date)

		if kol_tr > 0 {
			n := kol_tr * numFields

			query += `,(`
			for j := 0; j < 22; j++ {
				query += `$` + strconv.Itoa(n+j+1) + `,`
			}
			query = query[:len(query)-1] + `)`
		}

		if kol_ab%5000 == 0 {
			log.Println("received from client: ", kol_ab)
		}

		if kol_ab%100 == 0 {
			tx.MustExec(query, values...)
			tx.Commit()
			tr = true
			kol_tr = 0
		}
	}
}

/*func wsHandlerOld(w http.ResponseWriter, r *http.Request) {
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

		//conn.SetReadDeadline(time.Now().Add(60 * time.Second))

		if kol_ab%5000 == 0 {
			log.Println("received from client: ", kol_ab)
		}
		tx.Commit()
	}
}*/
