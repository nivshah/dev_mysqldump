package main

import (
	"bytes"
	"database/sql"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"crypto/x509"
	"crypto/tls"
    "gopkg.in/yaml.v2"
	"github.com/go-sql-driver/mysql"
)

func handleError(err interface{}) {
	if err != nil {
		log.Fatal(err)
	}
}

func analyze(user, host, port, password, database, ssl_ca string, db *sql.DB) {
	// To get biggest tables
	query := "SELECT table_name, coalesce(round(((data_length + index_length) / 1024 / 1024), 2), 0.00)"
	query += "FROM information_schema.TABLES WHERE table_schema = ? ORDER BY (data_length + index_length) DESC"
	rows, err := db.Query(query, database)

	handleError(err)
	defer rows.Close()

	var table_name string
	var table_size float64
	big_table_size := 100.0

	for rows.Next() {
		err := rows.Scan(&table_name, &table_size)
		handleError(err)
		if table_size > big_table_size {
			// Could this be done from existing connection?
			mysql_cmd := "mysql --user " + user + " --host " + host + " --port " + port + " -p" + password + " "

			if ssl_ca != "" {
				mysql_cmd += "--ssl-ca " + ssl_ca + " "
			}

			mysql_cmd += "--table --execute 'DESCRIBE " + table_name + ";' " + database

			out, err := exec.Command("/bin/bash", "-c", mysql_cmd).Output()
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("%v is %f mb! Figure out a way to make it smaller.\n%s\n", table_name, table_size, out)
		} else {
			log.Printf("%v is only %f mb - no problem.\n", table_name, table_size)
		}
	}

	err = rows.Err()
	handleError(err)

}

func dump(user, host, port, password, database, config_file, ssl_ca string, db *sql.DB) {
	type DbDumpConfig struct {
		Tables []struct {
			TableName string `yaml:"table_name"`
			Where     string `yaml:"where"`
			Flags     string `yaml:"flags"`
		}
	}
	data, err := ioutil.ReadFile(config_file)
	handleError(err)

	var config DbDumpConfig
	err = yaml.Unmarshal(data, &config)
	handleError(err)

	type Table struct {
		table_name string
		where      string
		flags      string
	}
	db_tables := []Table{}

	var table_name string
	rows, err := db.Query("SELECT table_name FROM information_schema.TABLES WHERE table_type='BASE TABLE' AND table_schema = ?", database)
	handleError(err)
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&table_name)
		switch err.(type) {
		default:
		case error:
			log.Fatal(err)
		}

		where := "1=1"
		flags := ""
		for i := 0; i < len(config.Tables); i++ {
			if config.Tables[i].TableName == table_name {
				where = config.Tables[i].Where
				flags = config.Tables[i].Flags
				break
			}
		}

		db_tables = append(db_tables, Table{table_name, where, flags})
	}

	outfile, err := os.Create("./output.sql")
	handleError(err)
	defer outfile.Close()

	// Add a create databse command to the dump file
    // This allows us to restore multiple databases at once
	outfile.WriteString("CREATE DATABASE " + database + ";\n")
	outfile.WriteString("USE " + database + ";\n")

	for i := 0; i < len(db_tables); i++ {
		table := db_tables[i]
		log.Println("Running mysql_dump for", table.table_name)
		command := "mysqldump --lock-tables=false --compact "
		command += "--host " + host + " --port " + port + " "
		command += "--user " + user + " -p" + password + " "

		if ssl_ca != "" {
			command += "--ssl-ca " + ssl_ca + " "
		}

		command += "--where=\"" + table.where + "\" "
		command += table.flags + " "
		command += database + " " + table.table_name

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
	command := "mysql --host " + host + " --port " + port + " --user " + user + " -p" + password + " "

	if ssl_ca != "" {
		command += "--ssl-ca " + ssl_ca + " "
	}

	command += "INFORMATION_SCHEMA  --skip-column-names --batch "
	command += "-e \"select table_name from tables where table_type = 'VIEW' and table_schema = '" + database + "'\""
	command += "| xargs mysqldump --host " + host + " --port " + port + " --user " + user + " -p" + password + " " + database + " "

	if ssl_ca != "" {
		command += "--ssl-ca " + ssl_ca + " "
	}
	// And get rid of the DEFINER statements on the views, because they end up causing 'access denied' issues
	command += "| sed -e 's/DEFINER[ ]*=[ ]*[^*]*\\*/\\*/'"
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

func main() {
	var user = flag.String("user", "root", "The mysql user")
	var host = flag.String("host", "localhost", "The mysql host")
	var port = flag.String("port", "3306", "The mysql port")
	var password = flag.String("password", "", "the password for this user")
	var database = flag.String("database", "", "The database name")
	var ssl_ca = flag.String("ssl-ca", "", "Path to SSL cert (if used)")
	var mode = flag.String("mode", "analyze", "Valid options are 'analyze' or 'dump")
	var config_file = flag.String("config", "", "The yaml config for 'dump' mode")
	flag.Parse()

	tlsString := "false"

	if *ssl_ca != "" {
		// we have a cert
		rootCertPool := x509.NewCertPool()
		pem, err := ioutil.ReadFile(*ssl_ca)
		if err != nil {
	       log.Fatal(err)
	    }

		if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
			log.Fatal("Failed to append PEM.")
	    }

		mysql.RegisterTLSConfig("custom", &tls.Config{
			ServerName: *host,
			RootCAs: rootCertPool,
		})

		tlsString = "custom"
	}

	db, err := sql.Open("mysql", *user+":"+*password+"@("+*host+":"+*port+")/"+*database+"?tls="+tlsString)
	handleError(err)
	defer db.Close()

	err = db.Ping()
	handleError(err)

	switch *mode {
	case "analyze":
		analyze(*user, *host, *port, *password, *database, *ssl_ca, db)
	case "dump":
		dump(*user, *host, *port, *password, *database, *ssl_ca, *config_file, db)
	default:
		log.Fatal("No valid mode provided! Please seee --help")
	}

}
