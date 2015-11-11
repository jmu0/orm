package dbmodel

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmu0/settings"
	"log"
	"os"
	"strconv"
	"strings"
)

//connect to database
func Connect() (*sql.DB, error) {
	//TODO change path
	set := settings.Settings{File: "orm.conf"}
	database, err := set.Get("database")
	if err != nil {
		fmt.Println(err)
		fmt.Printf("Database server: ")
		fmt.Scanln(&database)
	}
	usr, err := set.Get("user")
	if err != nil {
		fmt.Println(err)
		fmt.Printf("Username: ")
		fmt.Scanln(&usr)
	}
	pwd, err := set.Get("password")
	if err != nil {
		fmt.Println(err)
		fmt.Printf("Password: ")
		fmt.Scanln(&pwd)
	}
	db, err := sql.Open("mysql", makeDSN(database, usr, pwd))
	if err != nil {
		return db, err
	}
	return db, nil
}

//Get slice of map[string]string from database
func Query(db *sql.DB, query string) ([]map[string]string, error) {
	res := []map[string]string{}
	rows, err := db.Query(query)
	if err != nil {
		return res, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return res, err
	}
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		rows.Scan(scanArgs...)
		v := map[string]string{}
		var value string
		for i, col := range values {
			if col == nil {
				value = "NULL"
			} else {
				value = string(col)
			}
			v[columns[i]] = value
		}
		res = append(res, v)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return res, err
	}
	return res, nil
}

type DbObject interface {
	GetDbInfo() (dbName string, tblName string)
	GetColumns() []Column
}

//escape string to prevent common sql injection attacks
func Escape(str string) string {
	// ", ', 0=0
	str = strings.Replace(str, "\"", "\\\"", -1)
	str = strings.Replace(str, "'", "''", -1)
	// \x00, \n, \r, \ and \x1a"
	str = strings.Replace(str, "\x00", "", -1)
	str = strings.Replace(str, "\n", "", -1)
	str = strings.Replace(str, "\r", "", -1)
	str = strings.Replace(str, "\r", "", -1)
	//multiline attack
	str = strings.Replace(str, ";", " ", -1)
	//comments attack
	str = strings.Replace(str, "--", "", -1)
	str = strings.Replace(str, "#", "", -1)
	str = strings.Replace(str, "/*", "", -1)
	str = strings.Replace(str, "*/", "", -1)
	return str
}

//database object to map
func ToMap(obj DbObject) map[string]interface{} {
	cols := obj.GetColumns()
	m := make(map[string]interface{})
	for _, col := range cols {
		m[col.Field] = col.Value
	}
	return m
}

//Save database object
func Save(obj DbObject) (int, error) {
	dbName, tblName := obj.GetDbInfo()
	cols := obj.GetColumns()
	db, err := Connect()
	if err != nil {
		return 1, err
	}
	defer db.Close()
	query := "insert into " + dbName + "." + tblName + " "
	fields := "("
	values := "("
	update := ""
	for i, c := range cols {
		if len(fields) > 1 && i < len(cols) {
			fields += ", "
			values += ", "
			update += ", "
		}
		fields += c.Field
		values += valueString(c.Value)
		update += c.Field + "=" + valueString(c.Value)

	}
	fields += ") "
	values += ") "
	query += fields + " values " + values
	query += " on duplicate key update " + update
	// fmt.Println(query)
	_, err = db.Exec(query)
	// res, err := db.Exec(query)
	// fmt.Println(res.LastInsertId())
	// fmt.Println("result:", res)
	if err != nil {
		return 1, err
	}
	return 0, nil
}
func Delete(obj DbObject) (int, error) {
	dbName, tblName := obj.GetDbInfo()
	cols := obj.GetColumns()
	db, err := Connect()
	if err != nil {
		return 1, err
	}
	defer db.Close()
	query := "delete from " + dbName + "." + tblName + " where"
	where, err := strPrimaryKeyWhereSql(cols)
	if err != nil {
		fmt.Println("error:", err)
	}
	query += where
	// fmt.Println(query)
	res, err := db.Exec(query)
	if err != nil {
		return 1, err
	}
	nrrows, err := res.RowsAffected()
	if nrrows < 1 {
		return 1, errors.New("No rows deleted")
	}
	return 0, nil
}

//return string for query for value
func valueString(val interface{}) string {
	var value string
	// fmt.Println("valueString:", val)
	switch t := val.(type) {
	case string:
		// values += "\"" + reflect.ValueOf(c.Field) + "\""
		value += "\"" + Escape(val.(string)) + "\""
	case int, int32, int64:
		value += strconv.Itoa(val.(int))
	default:
		fmt.Println(t)
		value += "\"" + Escape(val.(string)) + "\""
	}
	// fmt.Println("Escaped:", value)
	return value
}

//returns connection string for database driver
func makeDSN(server, user, password string) string {
	var port string = "3306"
	return user + ":" + password + "@tcp(" + server + ":" + port + ")/"
}

//Get database names from server
func GetDatabaseNames(db *sql.DB) []string {
	dbs := []string{}
	query := "show databases"
	rows, err := db.Query(query)
	defer rows.Close()
	if err == nil && rows != nil {
		dbName := ""
		for rows.Next() {
			rows.Scan(&dbName)
			if !skipDb(dbName) {
				dbs = append(dbs, dbName)
			}
		}
	}
	return dbs
}

//don't show system databases
func skipDb(name string) bool {
	skip := []string{
		"information_schema",
		"mysql",
		"performance_schema",
		"owncloud",
		"roundcubemail",
	}
	for _, s := range skip {
		if name == s {
			return true
		}
	}
	return false
}

//Get table names from database
func GetTableNames(db *sql.DB, dbName string) []string {
	tbls := []string{}
	query := "show tables in " + dbName
	rows, err := db.Query(query)
	defer rows.Close()
	if err == nil && rows != nil {
		tableName := ""
		for rows.Next() {
			rows.Scan(&tableName)
			tbls = append(tbls, tableName)
		}
	}
	return tbls
}

//get list of columns from database table
func GetColumns(db *sql.DB, dbName string, tableName string) []Column {
	cols := []Column{}
	query := "show columns from " + dbName + "." + tableName
	rows, err := db.Query(query)
	defer rows.Close()
	if err == nil && rows != nil {
		col := Column{}
		for rows.Next() {
			rows.Scan(&col.Field, &col.Type, &col.Null, &col.Key, &col.Default, &col.Extra)
			cols = append(cols, col)
		}
	}
	return cols
}

//Structure to represent table column
type Column struct {
	Field   string
	Type    string
	Null    string
	Key     string
	Default string
	Extra   string
	Value   interface{}
}

//find out data type for database typ
func getType(t string) string {
	//TODO: more datatypes
	var dataTypes map[string]string = map[string]string{
		"varchar":  "string",
		"tinyint":  "int",
		"smallint": "int",
		"datetime": "string",
		"int":      "int",
	}
	t = strings.Split(t, "(")[0]
	if tp, ok := dataTypes[t]; ok {
		return tp
	} else {
		return "string"
	}
}

//find out if the class has int columns, then it neets strconv import
func needsStrconv(cols []Column) bool {
	for _, c := range cols {
		if getType(c.Type) == "int" {
			return true
		}
	}
	return false
}

//create object from db/table
func CreateObject(db *sql.DB, dbName, tblName string) error {
	var code string = ""
	var importPrefix = "github.com/jmu0/orm/"
	cols := GetColumns(db, dbName, tblName)

	code += "package " + strings.ToLower(tblName) + "\n\n"
	code += "import (\n\t\"" + importPrefix + "dbmodel\"\n"
	code += "\t\"errors\"\n"
	if needsStrconv(cols) {
		code += "\t\"strconv\"\n"
	}
	code += ")\n\n"
	code += "type " + tblName + " struct {\n"
	for _, col := range cols {
		code += "\t" + strings.ToUpper(col.Field[:1]) + col.Field[1:] + " " + getType(col.Type) + "\n"
	}
	code += "}\n\n"
	code += "func (" + strings.ToLower(tblName[:1]) + " *" + tblName + ") GetDbInfo() (dbName string, tblName string) {\n"
	code += "\treturn \"" + dbName + "\", \"" + tblName + "\"\n"
	code += "}\n\n"
	code += strGetQueryFunction(cols, dbName, tblName)
	code += strGetSaveFunction(cols, dbName, tblName)
	code += strGetDeleteFunction(cols, dbName, tblName)
	code += strGetColsFunction(cols, tblName)

	//Write to file
	folder := strings.ToLower(dbName) + "/" + strings.ToLower(tblName)
	err := os.MkdirAll(folder, 0770)
	if err != nil {
		log.Fatal(err)
	}
	path := folder + "/" + strings.ToLower(tblName) + ".go"
	file, err := os.Create(path)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	_, err = file.WriteString(code)
	if err != nil {
		log.Fatal(err)
	}
	return nil
}
func strGetColsFunction(c []Column, tblName string) string {
	var ret string
	ret = "func (" + strings.ToLower(tblName[:1]) + " *" + tblName + ") GetColumns() []dbmodel.Column {\n"
	ret += "\treturn []dbmodel.Column{\n"
	for _, col := range c {
		ret += "\t\t{\n"
		ret += "\t\t\tField:\"" + col.Field + "\",\n"
		ret += "\t\t\tType:\"" + col.Type + "\",\n"
		ret += "\t\t\tNull:\"" + col.Null + "\",\n"
		ret += "\t\t\tKey:\"" + col.Key + "\",\n"
		ret += "\t\t\tDefault:\"" + col.Default + "\",\n"
		ret += "\t\t\tExtra:\"" + col.Extra + "\",\n"
		ret += "\t\t\tValue: " + strings.ToLower(tblName)[:1] + "."
		ret += strings.ToUpper(col.Field[:1]) + col.Field[1:] + ",\n"
		ret += "\t\t},\n"
	}
	ret += "\t}\n"
	ret += "}\n\n"
	return ret
}
func strGetSaveFunction(c []Column, dbName string, tblName string) string {
	var ret string
	ret = "func (" + strings.ToLower(tblName[:1]) + " *" + tblName + ") Save() (Nr int, err error) {\n"
	ret += "\treturn dbmodel.Save(" + strings.ToLower(tblName[:1]) + ")\n"
	ret += "}\n\n"
	return ret
}
func strGetDeleteFunction(c []Column, dbName string, tblName string) string {
	var ret string
	ret = "func (" + strings.ToLower(tblName[:1]) + " *" + tblName + ") Delete() (Nr int, err error) {\n"
	ret += "\treturn dbmodel.Delete(" + strings.ToLower(tblName[:1]) + ")\n"
	ret += "}\n\n"
	return ret
}
func strGetQueryFunction(cols []Column, dbName string, tblName string) string {
	var ret string = "func Query(where string, orderby string) ([]" + tblName + ", error) {\n"
	ret += "\tquery := \"select * from " + dbName + "." + tblName + "\"\n"
	ret += "\tif len(where) > 0 {\n\t\tquery += \" where \" + where\n\t}\n"
	ret += "\tif len(orderby) > 0 {\n\t\tquery += \" order by \" + orderby\n\t}\n"
	ret += "\tret := []" + tblName + "{}\n"
	ret += "\tdb, err := dbmodel.Connect()\n"
	ret += "\tdefer db.Close()\n"
	ret += "\tif err != nil {\n\t\treturn ret, err\n\t}\n"
	ret += "\tres,err := dbmodel.Query(db, query)\n"
	ret += "\tif err != nil {\n\t\treturn ret, err\n\t}\n"
	ret += "\tfor _, r := range res {\n"
	if needsStrconv(cols) {
		ret += "\t\tvar err error\n"
	}
	ret += "\t\tobj := " + tblName + "{}\n"
	for _, c := range cols {
		tp := getType(c.Type)
		if tp == "int" {
			ret += "\t\tobj." + strings.ToUpper(c.Field[:1]) + c.Field[1:] + ", err = "
			ret += "strconv.Atoi(r[\"" + c.Field + "\"])\n"
			ret += "\t\tif err != nil && r[\"" + c.Field + "\"] != \"NULL\""
			ret += " {\n\t\t\treturn ret, err\n\t\t}\n"
		} else {
			ret += "\t\tobj." + strings.ToUpper(c.Field[:1]) + c.Field[1:] + " = "
			ret += "r[\"" + c.Field + "\"]\n"
		}
	}
	ret += "\t\tret = append(ret, obj)\n"
	ret += "\t}\n"
	ret += "\tif len(ret) == 0 {\n\t\treturn ret, errors.New(\"No rows found\")\n\t}\n"
	ret += "\treturn ret, nil\n"
	ret += "}\n\n"
	return ret
}
func strPrimaryKeyWhereSql(cols []Column) (string, error) {
	var ret string
	for _, c := range cols {
		if c.Key == "PRI" {
			if len(ret) > 0 {
				ret += " and"
			}
			ret += " " + c.Field + " = " + valueString(c.Value)
		}
	}
	return ret, nil
}
