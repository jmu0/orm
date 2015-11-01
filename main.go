package main

import (
	"fmt"
	"log"
	"orm/dbmodel"
)

func main() {
	db, err := dbmodel.Connect()
	defer db.Close()
	if err != nil {
		log.Fatal(err)
	}
	dbs := dbmodel.GetDatabaseNames(db)
	fmt.Println("")
	for _, name := range dbs {
		fmt.Println(name)
	}
	fmt.Printf("Database Name (* = All): ")
	var dbName string = ""
	fmt.Scanln(&dbName)
	fmt.Println("")
	if dbName == "*" {
		fmt.Println("scanning all databases...")
		for _, dbName = range dbs {
			tbls := dbmodel.GetTableNames(db, dbName)
			for _, tblName := range tbls {
				fmt.Println("Processing", dbName+"."+tblName, "...")
				dbmodel.CreateObject(db, dbName, tblName)
			}
		}
	} else {
		if !isInSlice(dbs, dbName) {
			log.Fatal(dbName + " is not a database")
		}
		tbls := dbmodel.GetTableNames(db, dbName)
		for _, name := range tbls {
			fmt.Println(name)
		}
		fmt.Printf("Table Name (* = All): ")
		var tblName string = ""
		fmt.Scanln(&tblName)
		fmt.Println("")
		if tblName == "*" {
			fmt.Println("scanning all tables from", dbName, "...")
			for _, tblName = range tbls {
				fmt.Println("Processing", dbName+"."+tblName, "...")
				dbmodel.CreateObject(db, dbName, tblName)
			}
		} else {
			if !isInSlice(tbls, tblName) {
				log.Fatal(tblName + " is not a table in " + dbName)
			}
			fmt.Println("Processing", dbName+"."+tblName, "...")
			dbmodel.CreateObject(db, dbName, tblName)
		}
	}
}
func isInSlice(lst []string, search string) bool {
	for _, val := range lst {
		if val == search {
			return true
		}
	}
	return false
}
