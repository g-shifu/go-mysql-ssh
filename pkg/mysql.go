package pkg

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"golang.org/x/crypto/ssh"
)

var sqlClient *sql.DB
var sshRemote *ssh.Client
var sshConnect bool

var BytesKind = reflect.TypeOf(sql.RawBytes{}).Kind()

type SSHDialer struct {
	client *ssh.Client
}

type Config struct {
	USE_SSH            bool
	SSH_REMOTE         string
	SSH_USER           string
	SSH_PASS           string
	DB_HOST            string
	DB_PORT            uint32
	DB_USER            string
	DB_PASS            string
	DB_SELECT          string
	DB_MAXOPENCONNS    int
	DB_MAXIDLECONNS    int
	DB_CONNMAXLIFETIME int
	DB_CHARTSET        string
	DB_TIMEOUT         uint32
}

func (sshDialer *SSHDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	return sshDialer.client.Dial("tcp", addr)
}

func InitDB(cfg *Config) {
	var dataSource string
	var err error
	var dialContext string = "tcp"
	if cfg.USE_SSH {
		if cfg.SSH_REMOTE == "" {
			cfg.SSH_REMOTE = userInput("Please enter the SSH tunnel address :")
		}
		if cfg.SSH_USER == "" {
			cfg.SSH_USER = userInput("Please enter the ssh user :")
		}
		if cfg.SSH_PASS == "" {
			cfg.SSH_PASS = userInput(fmt.Sprintf("Please enter the password for ssh user '%s' :", cfg.SSH_USER))
		}
		sshRemote, err = SSHClient(cfg)
		if err != nil {
			panic(fmt.Errorf("ssh connect error: %w n", err))
		}
		dialContext = "mysql+tcp"
		mysql.RegisterDialContext(dialContext, (&SSHDialer{sshRemote}).Dial)
		sshConnect = true
	}
	dataSource = fmt.Sprintf(
		"%s:%s@%s(%s:%d)/%s?charset=%s&timeout=%dms",
		cfg.DB_USER,
		cfg.DB_PASS,
		dialContext,
		cfg.DB_HOST,
		cfg.DB_PORT,
		cfg.DB_SELECT,
		cfg.DB_CHARTSET,
		cfg.DB_TIMEOUT,
	)
	sqlClient, err = sql.Open("mysql", dataSource)
	if err != nil {
		log.Fatal("Error")
	}
	sqlClient.SetConnMaxLifetime(time.Duration(cfg.DB_CONNMAXLIFETIME) * time.Second)
	sqlClient.SetMaxOpenConns(cfg.DB_MAXOPENCONNS)
	sqlClient.SetMaxIdleConns(cfg.DB_MAXIDLECONNS)
}

func NewConfig() *Config {
	return &Config{
		DB_PORT:            3306,
		DB_MAXOPENCONNS:    2,
		DB_CONNMAXLIFETIME: 2,
		DB_CHARTSET:        "utf8mb4",
		DB_TIMEOUT:         5000,
		USE_SSH:            false,
	}
}

func userInput(placeholder string) string {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println(placeholder)
	fmt.Print("-> ")
	input, err := reader.ReadString('\n')
	if err != nil {
		panic(fmt.Errorf("read input err : %w n", err))
	}
	input = strings.ReplaceAll(input, "\n", "")
	return input
}

func CloseDB() {
	sqlClient.Close()
	fmt.Println("mysql closeing..")
	if sshConnect {
		fmt.Println("ssh tunnel closeing..")
		sshRemote.Close()
	}
}

func checkErr(err error) {
	if err != nil {
		fmt.Printf("check error :%v", err)
	}
}

func ToStr(strObj interface{}) string {
	switch v := strObj.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", strObj)
	}
}

func ToFloat(floatObj interface{}) float64 {
	switch v := floatObj.(type) {
	case []byte:
		return float64(ToInt(string(v)))
	case float32:
		return float64(v)
	case float64:
		return float64(v)
	default:
		return 0.0
	}
}

func ToInt(intObj interface{}) int {
	switch v := intObj.(type) {
	case []byte:
		return ToInt(string(v))
	case int:
		return v
	case int8:
		return int(v)
	case int16:
		return int(v)
	case int32:
		return int(v)
	case int64:
		return int(v)
	case uint:
		return int(v)
	case uint8:
		return int(v)
	case uint16:
		return int(v)
	case uint32:
		return int(v)
	case uint64:
		if v > math.MaxInt64 {
			info := fmt.Sprintf("ToInt, error, overflowd %v", v)
			panic(info)
		}
		return int(v)
	case string:
		strv := v
		if strings.Contains(v, ".") {
			strv = strings.Split(v, ".")[0]
		}
		if strv == "" {
			return 0
		}
		if intv, err := strconv.Atoi(strv); err == nil {
			return intv
		}
	}
	return 0
}

func RowsToMap(rows *sql.Rows) []map[string]interface{} {
	result := make([]map[string]interface{}, 0)

	for rows.Next() {
		cols, err := rows.Columns()
		checkErr(err)

		colsTypes, err := rows.ColumnTypes()
		checkErr(err)

		dest := make([]interface{}, len(cols))
		destPointer := make([]interface{}, len(cols))
		for i := range dest {
			destPointer[i] = &dest[i]
		}

		err = rows.Scan(destPointer...)
		checkErr(err)

		rowResult := make(map[string]interface{})
		for i, colVal := range dest {
			colName := cols[i]
			itemType := colsTypes[i].ScanType()
			// fmt.Println(itemType.Kind() == reflect.Uint64)
			// fmt.Printf("%s type %v \n", colName, itemType)
			switch itemType.Kind() {
			case BytesKind:
				rowResult[colName] = ToStr(colVal)

			case reflect.Int, reflect.Int8,
				reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				rowResult[colName] = ToInt(colVal)
			case reflect.Float32, reflect.Float64:
				rowResult[colName] = ToFloat(colVal)
			case reflect.Struct:
				if itemType.Name() == "NullTime" {
					rowResult[colName] = ToStr(colVal)
				} else {
					rowResult[colName] = ToInt(colVal)
				}
			default:
				rowResult[colName] = ToStr(colVal)
			}
		}
		result = append(result, rowResult)
	}
	rows.Close()
	return result
}

func OneRowToMap(rows *sql.Rows) map[string]interface{} {
	rowResult := make(map[string]interface{})
	if rows.Next() {
		cols, err := rows.Columns()
		checkErr(err)

		colsTypes, err := rows.ColumnTypes()
		checkErr(err)

		dest := make([]interface{}, len(cols))
		destPointer := make([]interface{}, len(cols))
		for i := range dest {
			destPointer[i] = &dest[i]
		}

		err = rows.Scan(destPointer...)
		checkErr(err)

		for i, colVal := range dest {
			colName := cols[i]
			itemType := colsTypes[i].ScanType()
			switch itemType.Kind() {
			case BytesKind:
				rowResult[colName] = ToStr(colVal)

			case reflect.Int, reflect.Int8,
				reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:

				rowResult[colName] = ToInt(colVal)
			case reflect.Struct:
				if itemType.Name() == "NullTime" {
					rowResult[colName] = ToStr(colVal)
				} else {
					rowResult[colName] = ToInt(colVal)
				}
			default:
				rowResult[colName] = ToStr(colVal)
			}
		}
	}
	rows.Close()
	return rowResult
}

func DBQueryRows(sqlStr string) ([]map[string]interface{}, error) {
	dbRows, err := sqlClient.Query(sqlStr)
	if err != nil {
		panic(err.Error())
	}
	rows := RowsToMap(dbRows)
	return rows, err
}

func DBQueryOneRow(sqlStr string) (map[string]interface{}, error) {
	dbRows, err := sqlClient.Query(sqlStr)
	if err != nil {
		panic(err.Error())
	}
	row := OneRowToMap(dbRows)
	return row, err
}

func DBUpdate(sqlStr string) (int64, error) {
	result, err := sqlClient.Exec(sqlStr)
	if err != nil {
		panic(err.Error())
	}
	rowsAffected, _ := result.RowsAffected()
	return rowsAffected, err
}

func DBUpdatePretreat(sqlStr string, args ...any) (int64, error) {
	stmt, _ := sqlClient.Prepare(sqlStr)
	defer stmt.Close()
	result, err := stmt.Exec(args...)
	if err != nil {
		panic(err.Error())
	}
	rowsAffected, _ := result.RowsAffected()
	return rowsAffected, err
}

func DBInsertPretreat(sqlStr string, args ...any) (int64, error) {
	stmt, _ := sqlClient.Prepare(sqlStr)
	defer stmt.Close()
	result, err := stmt.Exec(args...)
	if err != nil {
		panic(err.Error())
	}
	id, _ := result.LastInsertId()
	return id, err
}

func DBInsert(sqlStr string) (int64, error) {
	result, err := sqlClient.Exec(sqlStr)
	if err != nil {
		panic(err.Error())
	}
	id, _ := result.LastInsertId()
	return id, err
}

func SSHClient(cfg *Config) (sshtun *ssh.Client, err error) {
	sshConfig := &ssh.ClientConfig{
		User: cfg.SSH_USER,
		Auth: []ssh.AuthMethod{
			ssh.Password(cfg.SSH_PASS),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         time.Second * 10,
	}
	sshtun, err = ssh.Dial("tcp", cfg.SSH_REMOTE, sshConfig)
	return sshtun, err
}
