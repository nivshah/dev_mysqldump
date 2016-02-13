package main

import (
	"bytes"
	"database/sql"
	"flag"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
)

func handleError(err interface{}) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	var user = flag.String("user", "root", "the mysql user")
	var host = flag.String("host", "localhost", "the mysql host")
	var port = flag.String("post", "3306", "the mysql post")
	var password = flag.String("password", "", "the password for this user")
	var database = flag.String("database", "", "the database name")
	var config_file = flag.String("config", "", "the yaml config")
	flag.Parse()

	db, err := sql.Open("mysql", *user+":"+*password+"@("+*host+":"+*port+")/"+*database)
	handleError(err)
	defer db.Close()

	err = db.Ping()
	handleError(err)

	type DbDumpConfig struct {
		Tables []struct {
			TableName string `yaml:"table_name"`
			Where     string `yaml:"where"`
		}
	}
	data, err := ioutil.ReadFile(*config_file)
	handleError(err)

	var config DbDumpConfig
	err = yaml.Unmarshal(data, &config)
	handleError(err)

	type Table struct {
		table_name string
		where      string
	}
	db_tables := []Table{}

	var (
		table_name string
	)
	rows, err := db.Query("SELECT table_name FROM information_schema.TABLES WHERE table_schema = ?", *database)
	handleError(err)
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&table_name)
		switch err := err.(type) {
		default:
		case error:
			log.Fatal(err)
		}

		where := "1=1"
		for i := 0; i < len(config.Tables); i++ {
			if config.Tables[i].TableName == table_name {
				where = config.Tables[i].Where
				break
			}
		}

		db_tables = append(db_tables, Table{table_name, where})
	}

	outfile, err := os.Create("./output.sql")
	handleError(err)
	defer outfile.Close()

	for i := 0; i < len(db_tables); i++ {
		table := db_tables[i]
		log.Println("Running mysql_dump for", table.table_name)
		command := "mysqldump --lock-tables=false --compact "
		command += "--host " + *host + " --user " + *user + " -p" + *password + " "
		command += "--where=\"" + table.where + "\" "
		command += *database + " " + table.table_name

		cmd := exec.Command("/bin/bash", "-c", command)
		cmd.Stdout = outfile
		var errBuff bytes.Buffer
		cmd.Stderr = &errBuff

		err = cmd.Start()
		handleError(err)

		cmd.Wait()
		if errBuff.Len() > 0 {
			log.Printf("\n%s", errBuff.String())
		}
	}

	// Dump the views too
	command := "mysql --host " + *host + " --user " + *user + " -p" + *password + " "
	command += "INFORMATION_SCHEMA  --skip-column-names --batch "
	command += "-e \"select table_name from tables where table_type = 'VIEW' and table_schema = '" + *database + "'\""
	command += "| xargs mysqldump --host " + *host + " --user " + *user + " -p" + *password + " " + *database
	log.Println("Cmd: ", command)

	cmd := exec.Command("/bin/bash", "-c", command)
	cmd.Stdout = outfile
	var errBuff bytes.Buffer
	cmd.Stderr = &errBuff

	err = cmd.Start()
	handleError(err)

	cmd.Wait()
	if errBuff.Len() > 0 {
		log.Printf("\n%s", errBuff.String())
	}
}
