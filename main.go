package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type JsonStruct struct {
	ExplainFq json.RawMessage `json:"explain_fq"`
	Data      [][]interface{} `json:"data"`
	Keys      []string        `json:"keys"`
}

type ExplainFq struct {
	Table string `json:"table"`
}

var tableOrder = []string{"Customers", "Addresses", "ContractAccounts", "Contracts", "Products"}

func main() {
	db, err := sql.Open("mysql", "root:@tcp(127.0.0.1:3306)/gwleak")
	if err != nil {
		log.Panicln("Error opening database:", err)
	}
	defer db.Close()

	files, err := filepath.Glob("E:/gwleak/data/*.json")
	if err != nil {
		log.Panicln("Error finding JSON files:", err)
	}

	tableData := make(map[string][]JsonStruct)

	for _, file := range files {
		jsonData := readFile(file)
		if jsonData != nil {
			tableName := getTableName(jsonData.ExplainFq)
			tableData[tableName] = append(tableData[tableName], *jsonData)
		}
	}

	// Insert data based on tableOrder order
	for _, tableName := range tableOrder {
		if data, ok := tableData[tableName]; ok {
			for _, jsonData := range data {
				insertData(db, tableName, jsonData.Keys, jsonData.Data)
			}
		}
	}
}

func readFile(file string) *JsonStruct {
	content, err := os.ReadFile(file)
	if err != nil {
		log.Println("Error reading JSON file:", err)
		return nil
	}

	var jsonData JsonStruct
	err = json.Unmarshal(content, &jsonData)
	if err != nil {
		log.Println("Error unmarshalling JSON:", err)
		return nil
	}

	return &jsonData
}

func getTableName(explainFq json.RawMessage) string {
	var explain []ExplainFq
	err := json.Unmarshal(explainFq, &explain)
	if err != nil || len(explain) == 0 {
		return ""
	}
	return explain[0].Table
}

func insertData(db *sql.DB, tableName string, keys []string, data [][]interface{}) {
	placeholders := make([]string, len(keys))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(keys, ", "),
		strings.Join(placeholders, ", "))

	stmt, err := db.Prepare(query)
	if err != nil {
		log.Printf("Error preparing statement for table %s: %v\n", tableName, err)
		return
	}
	defer stmt.Close()

	for i, row := range data {
		_, err = stmt.Exec(row...)
		if err != nil {
			log.Printf("Error inserting data into table %s (index %d): %v\n", tableName, i, err)
		} else {
			log.Printf("Successfully inserted data into table %s (index %d)\n", tableName, i)
		}
	}

	log.Printf("Finished inserting data into table %s. Total rows processed: %d\n", tableName, len(data))
}
