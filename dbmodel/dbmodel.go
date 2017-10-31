package dbmodel

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	//used for connecting to datbase
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmu0/settings"
)

//Connect connect to database
func Connect(arg ...string) (*sql.DB, error) {
	//TODO change path
	var path string
	path = "orm.conf"
	if _, err := os.Stat(path); os.IsNotExist(err) {
		path = "/etc/orm.conf"
	}
	set := settings.Settings{File: path}
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
	//db.SetMaxOpenConns(50)
	//db.SetMaxIdleConns(20)
	// d, _ := time.ParseDuration("1 second")
	// db.SetConnMaxLifetime(d)
	if err != nil {
		return db, err
	}
	return db, nil
}

//Query Get slice of map[string]interface{} from database
func Query(db *sql.DB, query string) ([]map[string]interface{}, error) {
	res := make([]map[string]interface{}, 0)
	rows, err := db.Query(query)
	if err != nil {
		return res, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		rows.Close()
		return res, err
	}
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		rows.Scan(scanArgs...)
		v := make(map[string]interface{})
		var value interface{}
		for i, col := range values {
			if col == nil {
				value = ""
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
	//DEBUG:log.Println(res)
	return res, nil
}

//HandleREST handle REST api for DbObject
func HandleREST(pathPrefix string, w http.ResponseWriter, r *http.Request) string {
	var objStr = r.URL.Path
	db, err := Connect()
	if err != nil {
		http.Error(w, "REST: Could not connect to database", http.StatusInternalServerError)
		return ""
	}
	if pathPrefix[0] != '/' {
		pathPrefix = "/" + pathPrefix
	}
	objStr = strings.Replace(objStr, pathPrefix, "", 1)
	if objStr[0] == '/' {
		objStr = objStr[1:]
	}
	if objStr[len(objStr)-1] == '/' {
		objStr = objStr[:len(objStr)-1]
	}
	//fmt.Println("REST DEBUG: objStr:", objStr)
	objParts := strings.Split(objStr, "/")
	for key, value := range objParts {
		objParts[key] = Escape(value)
	}
	switch len(objParts) {
	case 1: //only db, write list of tables
		if r.Method == "GET" {
			tbls := GetTableNames(db, objParts[0])
			if len(tbls) > 0 {
				bytes, err := json.Marshal(tbls)
				if err != nil {
					fmt.Println("HandleRest: error encoding json:", err)
					http.Error(w, "Could not encode json", http.StatusInternalServerError)
					return ""
				}
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				w.Write(bytes)
			} else {
				http.Error(w, "Database doesn't exist", http.StatusNotFound)
				return ""
			}
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return ""
		}
	case 2: //table, query rows
		if r.Method == "GET" {
			q := "select * from " + objParts[0] + "." + objParts[1]
			//TODO check for query
			if where, ok := r.URL.Query()["q"]; ok != false {
				q += " where " + Escape(where[0])
				q = strings.Replace(q, "''", "'", -1)
			}
			log.Println("DEBUG: REST query:", q)
			writeQueryResults(db, q, w)
		} else if r.Method == "POST" { //post to a db table url
			cols := getColsWithValues(db, objParts[0], objParts[1], r)
			if len(cols) == 0 {
				http.Error(w, "REST: Object not found", http.StatusNotFound)
				return ""
			}
			log.Println("POST:", r.URL.Path)
			n, id, err := save(objParts[0], objParts[1], cols)
			if err != nil {
				log.Println("REST ERROR: POST:", objParts, err)
				http.Error(w, "Could not save", http.StatusInternalServerError)
				return ""
			}
			if n == 1 && id > -1 {
				cols = setAutoIncColumn(id, cols)
			}
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.Write([]byte("{\"n\":\"" + strconv.Itoa(n) + "\",\"id\":\"" + strconv.Itoa(id) + "\"}"))
			json, err := cols2json(objParts[1], cols)
			if err != nil {
				return ""
			}
			return string(json)
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return ""
		}
		return ""
	case 3: //table primary key, perform CRUD
		// fmt.Println("DEBUG: HandleRest:", cols)
		switch r.Method {
		case "GET":
			log.Println("REST: GET:", objParts)
			cols := getColsWithValues(db, objParts[0], objParts[1], r)
			//put primary key values in columns
			keys := strings.Split(objParts[2], ":")
			keyCounter := 0
			for index, column := range cols {
				if column.Key == "PRI" {
					cols[index].Value = keys[keyCounter]
					keyCounter++
					if keyCounter == len(keys) {
						break
					}
				}
			}
			q := "select * from " + objParts[0] + "." + objParts[1] + " where "
			where, err := strPrimaryKeyWhereSQL(cols)
			if err != nil {
				http.Error(w, "Could not build query", http.StatusInternalServerError)
				return ""
			}
			q += where
			log.Println("REST: GET: query ", q)
			writeQueryResults(db, q, w)
		case "POST": //post to a object id
			cols := getColsWithValues(db, objParts[0], objParts[1], r)
			if len(cols) == 0 {
				http.Error(w, "Object not found", http.StatusNotFound)
				return ""
			}
			//put primary key values in columns
			keys := strings.Split(objParts[2], ":")
			keyCounter := 0
			for index, column := range cols {
				if column.Key == "PRI" {
					cols[index].Value = keys[keyCounter]
					keyCounter++
					if keyCounter == len(keys) {
						break
					}
				}
			}
			log.Println("POST:", r.URL.Path)
			log.Println("DEBUG POST:", cols)
			n, id, err := save(objParts[0], objParts[1], cols)
			if err != nil {
				log.Println("REST: ERROR: POST:", objParts, err)
				http.Error(w, "Could not save", http.StatusInternalServerError)
				return ""
			}
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.Write([]byte("{\"n\":\"" + strconv.Itoa(n) + "\",\"id\":\"" + strconv.Itoa(id) + "\"}"))
			json, err := cols2json(objParts[1], cols)
			if err != nil {
				return ""
			}
			return string(json)
		case "DELETE":
			log.Println("REST: DELETE:", objParts)

			//TODO delete
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return ""
		}
	default:
		http.Error(w, "Invalid Path", http.StatusInternalServerError)
		return ""
	}
	return ""
}

func getColsWithValues(db *sql.DB, dbName string, tblName string, r *http.Request) []Column {
	cols := GetColumns(db, dbName, tblName)
	data, err := getRequestData(r)
	if err != nil {
		log.Println("REST: ERROR: POST:", dbName, tblName, err)
	}
	//set column values
	for key, value := range data {
		index := findColIndex(key, cols)
		if index > -1 {
			cols[index].Value = value
		}
	}
	return cols
}

func cols2json(table string, cols []Column) ([]byte, error) {
	var ret map[string]interface{}
	ret = make(map[string]interface{})
	ret["type"] = table
	for _, col := range cols {
		ret[col.Field] = col.Value
	}
	json, err := json.Marshal(ret)
	if err != nil {
		return []byte(""), err
	}
	return json, nil
}

func findColIndex(field string, cols []Column) int {
	for index, col := range cols {
		if col.Field == field {
			return index
		}
	}
	return -1
}
func writeQueryResults(db *sql.DB, q string, w http.ResponseWriter) {
	var ret interface{}
	res, err := Query(db, q)
	//fmt.Println("REST: DEBUG: writeQueryResults:", q)
	if err != nil {
		http.Error(w, "No results found", http.StatusNotFound)
		return
	}
	if len(res) == 1 {
		ret = res[0]
	} else {
		ret = res
	}
	bytes, err := json.Marshal(ret)
	if err != nil {
		fmt.Println("HandleRest: error encoding json:", err)
		http.Error(w, "No results found", http.StatusNotFound)
		return
	}
	//drop password fields
	var pwReg = ",\"?([P,p]ass[W,w]o?r?d|[W,w]acht[W,w]o?o?r?d?)\"?:\"(.*?)\""
	passwdReg := regexp.MustCompile(pwReg)
	str := passwdReg.ReplaceAllString(string(bytes), "")
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write([]byte(str))
}

//getRequestData get data from post request
func getRequestData(req *http.Request) (map[string]string, error) {
	err := req.ParseForm()
	if err != nil {
		return make(map[string]string), err
	}
	res := make(map[string]string)
	for k, v := range req.Form {
		res[k] = strings.Join(v, "")
	}
	return res, nil
}

//DbObject interface
type DbObject interface {
	GetDbInfo() (dbName string, tblName string)
	GetColumns() []Column
	Set(key string, value interface{}) error
	Save() (Nr int, err error)
	Delete() (Nr int, err error)
}

//Escape string to prevent common sql injection attacks
func Escape(str string) string {

	// ", ', 0=0
	str = strings.Replace(str, "\"", "\\\"", -1)
	str = strings.Replace(str, "''", "'", -1)
	str = strings.Replace(str, "'", "''", -1)

	// \x00, \n, \r, \ and \x1a"
	str = strings.Replace(str, "\x00", "", -1)
	str = strings.Replace(str, "\n", "", -1)
	str = strings.Replace(str, "\r", "", -1)
	str = strings.Replace(str, "\x1a", "", -1)

	//multiline attack
	str = strings.Replace(str, ";", " ", -1)

	//comments attack
	str = strings.Replace(str, "--", "", -1)
	str = strings.Replace(str, "#", "", -1)
	str = strings.Replace(str, "/*", "", -1)
	str = strings.Replace(str, "*/", "", -1)
	return str
}

//ToMap database object to map
func ToMap(obj DbObject) map[string]interface{} {
	cols := obj.GetColumns()
	m := make(map[string]interface{})
	for _, col := range cols {
		m[col.Field] = col.Value
	}
	return m
}

//ToMapSlice database objects to slice of maps
func ToMapSlice(slice []DbObject) []map[string]interface{} {
	ret := make([]map[string]interface{}, 0)
	for _, obj := range slice {
		ret = append(ret, ToMap(obj))
	}
	return ret
}

//Save database object using statement
func Save(obj DbObject) (int, error) {
	dbName, tblName := obj.GetDbInfo()
	cols := obj.GetColumns()
	n, _, err := save(dbName, tblName, cols)
	return n, err
}

//save can be used by HandleREST and DbObject
func save(dbName string, tblName string, cols []Column) (int, int, error) {
	var err error
	db, err := Connect()
	if err != nil {
		return -1, -1, err
	}
	defer db.Close()

	query := "insert into " + dbName + "." + tblName + " "
	fields := "("
	strValues := "("
	insValues := make([]interface{}, 0)
	updValues := make([]interface{}, 0)
	strUpdate := ""
	for _, c := range cols {
		//log.Println("DEBUG:", c)
		if c.Value != nil {
			if (getType(c.Type) == "int" && c.Value == "") == false { //skip auto_increment column
				if len(fields) > 1 {
					fields += ", "
				}
				fields += c.Field
				if len(strValues) > 1 {
					strValues += ", "
				}
				strValues += "?"
				insValues = append(insValues, c.Value)
				if len(strUpdate) > 0 {
					strUpdate += ", "
				}
				strUpdate += c.Field + "=?"
				updValues = append(updValues, c.Value)
			}
		}
	}
	fields += ")"
	strValues += ")"
	query += fields + " values " + strValues
	query += " on duplicate key update " + strUpdate
	// log.Println("DEBUG SAVE query:", query)
	insValues = append(insValues, updValues...)
	qr, err := db.Exec(query, insValues...)
	// stmt, err := db.Prepare(query)
	// if err != nil {
	// 	return -1, -1, err
	// }
	// qr, err := stmt.Exec(insValues...)
	if err != nil {
		return -1, -1, err
	}

	id, err := qr.LastInsertId()
	if err != nil {
		id = -1
	}
	n, err := qr.RowsAffected()
	if err != nil {
		n = -1
	}
	// fmt.Println("REST: DEBUG: save result n:", n, "id:", id)
	return int(n), int(id), nil
}

//SaveQuery (DEPRECATED) Save database object to database (insert or update) using insert query
func SaveQuery(obj DbObject) (int, error) {
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
	_, err = db.Exec(query)
	if err != nil {
		return 1, err
	}
	return 0, nil
}

//Delete database object from database
func Delete(obj DbObject) (int, error) {
	dbName, tblName := obj.GetDbInfo()
	cols := obj.GetColumns()
	db, err := Connect()
	if err != nil {
		return 1, err
	}
	defer db.Close()
	query := "delete from " + dbName + "." + tblName + " where"
	where, err := strPrimaryKeyWhereSQL(cols)
	if err != nil {
		fmt.Println("error:", err)
	}
	query += where
	res, err := db.Exec(query)
	if err != nil {
		return 1, err
	}
	nrrows, _ := res.RowsAffected()
	if nrrows < 1 {
		return 1, errors.New("No rows deleted")
	}
	return 0, nil
}

//return string for query for value
func valueString(val interface{}) string {
	var value string
	if val == nil {
		return ""
	}
	switch t := val.(type) {
	case string:
		value += "\"" + Escape(val.(string)) + "\""
	case int, int32, int64:
		value += strconv.Itoa(val.(int))
	default:
		fmt.Println(t)
		value += "\"" + Escape(val.(string)) + "\""
	}
	return value
}

//returns connection string for database driver
func makeDSN(server, user, password string) string {
	var port string
	port = "3306"
	return user + ":" + password + "@tcp(" + server + ":" + port + ")/"
}

//GetDatabaseNames Get database names from server
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

//GetTableNames Get table names from database
func GetTableNames(db *sql.DB, dbName string) []string {
	tbls := []string{}
	query := "show tables in " + dbName
	rows, err := db.Query(query)
	if err != nil {
		return tbls
	} else if rows != nil {
		tableName := ""
		for rows.Next() {
			rows.Scan(&tableName)
			tbls = append(tbls, tableName)
		}
	}
	defer rows.Close()
	return tbls
}

//GetColumns get list of columns from database table
func GetColumns(db *sql.DB, dbName string, tableName string) []Column {
	cols := []Column{}
	var col Column
	query := "show columns from " + dbName + "." + tableName
	//TODO: waarom zie ik geen auto_increment in kolom Extra??
	rows, err := db.Query(query)
	defer rows.Close()
	if err == nil && rows != nil {
		for rows.Next() {
			col = Column{}
			rows.Scan(&col.Field, &col.Type, &col.Null, &col.Key, &col.Default, &col.Extra)
			// fmt.Println("DEBUG:",rows)
			// fmt.Println("DEBUG GetColumns:", col)
			cols = append(cols, col)
		}
	}
	return cols
}

//Column Structure to represent table column
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
	var dataTypes map[string]string
	dataTypes = map[string]string{
		"varchar":  "string",
		"tinyint":  "int",
		"smallint": "int",
		"datetime": "string",
		"int":      "int",
	}
	t = strings.Split(t, "(")[0]
	if tp, ok := dataTypes[t]; ok {
		return tp
	}
	return "string"
}

func setAutoIncColumn(id int, cols []Column) []Column {
	//fmt.Println("DEBUG:setAutoIncColumn")
	for index, col := range cols {
		if strings.Contains(col.Type, "int") && col.Key == "PRI" {
			//fmt.Println("DEBUG:found", col.Field)
			cols[index].Value = id
		}
	}
	return cols
}

//find out if the class has int columns, then it neets strconv import
func hasIntColumns(cols []Column) bool {
	for _, c := range cols {
		if getType(c.Type) == "int" {
			return true
		}
	}
	return false
}

//CreateObject create object from db/table
func CreateObject(db *sql.DB, dbName, tblName string) error {
	var code string
	var importPrefix = "github.com/jmu0/orm/"
	cols := GetColumns(db, dbName, tblName)

	code += "package " + strings.ToLower(tblName) + "\n\n"
	code += "import (\n\t\"" + importPrefix + "dbmodel\"\n"
	code += "\t\"errors\"\n"
	if hasIntColumns(cols) {
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
	code += strGetSetFunction(cols, tblName)
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

func strGetSetFunction(c []Column, tblName string) string {
	var ret string
	var letter = strings.ToLower(tblName[:1])
	ret = "func (" + letter + " *" + tblName + ") Set(key string, value interface{}) error {\n"
	if hasIntColumns(c) {
		ret += "\tvar err error\n"
	}
	ret += "\tif  value == nil {\n"
	ret += "\t\treturn errors.New(\"value for \" + key + \" is nil\")\n"
	ret += "\t}\n"
	ret += "\tswitch key {\n"
	for _, col := range c {
		ret += "\tcase \"" + col.Field + "\":\n" //TODO: capitalize fields
		if getType(col.Type) == "int" {
			ret += "\t\t" + letter + "."
			ret += strings.ToUpper(col.Field[:1]) + col.Field[1:]
			ret += ", err = strconv.Atoi(value.(string))\n"
			ret += "\t\tif err != nil && value != \"NULL\" {\n"
			ret += "\t\t\treturn err\n"
			ret += "\t\t}\n"
		} else {
			ret += "\t\t" + letter + "." + strings.ToUpper(col.Field[:1]) + col.Field[1:] + " = value.(string)\n"
		}
		ret += "\t\treturn nil\n"
	}
	ret += "\tdefault:\n"
	ret += "\t\treturn errors.New(\"Key not found:\" + key)\n"
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
	//TODO: with this code integer fields cannot be null. change to check for ""
	var ret string
	ret = "func Query(where string, orderby string) ([]" + tblName + ", error) {\n"
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
	if hasIntColumns(cols) {
		ret += "\t\tvar err error\n"
	}
	ret += "\t\tobj := " + tblName + "{}\n"
	for _, c := range cols {
		tp := getType(c.Type)
		if tp == "int" {
			// ret += "\t\tobj." + strings.ToUpper(c.Field[:1]) + c.Field[1:] + " = "
			// ret += "r[\"" + c.Field + "\"].(int)\n"
			ret += "\t\tobj." + strings.ToUpper(c.Field[:1]) + c.Field[1:] + ", err = "
			ret += "strconv.Atoi(r[\"" + c.Field + "\"].(string))\n"
			ret += "\t\tif err != nil && r[\"" + c.Field + "\"] != \"NULL\""
			ret += " {\n\t\t\treturn ret, err\n\t\t}\n"
		} else {
			ret += "\t\tif r[\"" + c.Field + "\"] != nil {\n"
			ret += "\t\t\tobj." + strings.ToUpper(c.Field[:1]) + c.Field[1:] + " = "
			ret += "r[\"" + c.Field + "\"].(string)\n"
			ret += "\t\t}\n"
		}
	}
	ret += "\t\tret = append(ret, obj)\n"
	ret += "\t}\n"
	ret += "\tif len(ret) == 0 {\n\t\treturn ret, errors.New(\"No rows found\")\n\t}\n"
	ret += "\treturn ret, nil\n"
	ret += "}\n\n"
	return ret
}

func strPrimaryKeyWhereSQL(cols []Column) (string, error) {
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
