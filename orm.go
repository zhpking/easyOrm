package main

import (
	"database/sql"
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"strings"
)

type EasyDb struct {
	*sql.DB
}

/**
["name"] => ["test"]
["num >"] => ["1", "or"]
["age >"] => ["a"]
 */
/*
func (d *EasyDb) GetOne(m map[string][]interface{}) {
	// d.Db.Where("name = ? AND age >= ?", "jinzhu", "22").Find(&users)
	paramStr := ""
	valueInterface := []interface{}
	for field, value := range m {
		// 去空格
		field = strings.TrimSpace(field)
		// 如果包含空格
		if strings.Contains(field, " ") {
			// 用空格切割字符串
			fArr := strings.Split(field, " ")
			paramStr = fArr[0] + fArr[1]
		} else {
			// paramStr = field + "="
			switch m[field][0].(type) {
			case string:
				paramStr += "="
			case []string:
				paramStr += "in"
			}
		}

		// 处理value
		valueInterface = append(valueInterface, m[field][0])
	}
}
*/


/*
* 批量更新不同的字段值(update when case)
* 示例1：
* multipleData = {
*     map[string]string{"id":1, "name":"foo", "email":"foo@xxx.com"}
*     map[string]string{"id":2, "email":"bar"}
* }
* batchUpdate(multipleData)
*
* 更新操作描述为：
* 1.对关键字段 id 为 1 的记录，更新 name 为 foo，更新 email 为 foo@xxx.com
* 2.对关键字段 id 为 2 的记录，更新 name 为 bar
*
* 示例2：
* multipleData = []map[string]string{
*     map[string]string{"name":foo, "email":"foo@xxx.com"}
*     map[string]string{"name":bar, "phone":"1234567890"}
* }
*
* shareWhereField = []map[string]interface{}{
*     map[string]string{"type":[]{1,2}}
*     map[string]string{"type like":"666"}
* }
*
* batchUpdate(multipleData, shareWhereField);
*
* 更新操作描述为：
* 1.对关键字段 name 为 foo 且 type 为 1 或 2 的记录 且type2 为 "666"的记录，更新 email 为 foo@aipai.com
* 2.对关键字段 name 为 bar 且 type 为 1 或 2 的记录 且type2 为 "666"的记录，更新 phone 为 1234567890
**/
func (d *EasyDb) BatchUpdate(tableName string, multipleData []map[string]string, shareWhereField []map[string]interface{}) (sql.Result, error) {
	firstRow := multipleData[0]

	// 收集所有需更新的字段
	updateColumn := make([]string, 0)
	repeatMap := make(map[string]int, 0)
	for _, data := range multipleData {
		for field, _ := range data {
			// 去重
			if _, ok := repeatMap[field]; !ok {
				updateColumn = append(updateColumn, field)
				repeatMap[field] = 1
			}
		}
	}

	// 获取关键字段
	referenceColumn := updateColumn[0]
	if _, ok := firstRow["id"]; ok {
		referenceColumn = "id"
	}

	// 去掉关键字段（关键字段不需要更新）
	updateColumn = updateColumn[1:]
	updateValue := make([]interface{}, 0)

	// 更新sql
	updateSql := "UPDATE " + tableName + " SET "
	// when case sql
	setSql := ""
	// when case sql集合
	sets := make([]string, 0)
	// 关键字段搜索条件
	referenceWhere := "where " + referenceColumn + " in("
	// 关键字段搜索条件字段值
	referenceValue := make([]interface{}, 0)

	// 拼接when case 语句
	for _, uColumn := range updateColumn {
		setSql = "`" + uColumn + "` = CASE "
		for _, data := range multipleData {
			if _, ok := data[uColumn]; !ok {
				continue
			}
			setSql += "WHEN `" + referenceColumn + "` = ? THEN ? "
			updateValue = append(updateValue, data[referenceColumn], data[uColumn])

			// 主数据更新条件值
			referenceWhere += "?,"
			referenceValue = append(referenceValue, data[referenceColumn])
		}
		setSql += "ELSE `" + uColumn + "` END "
		sets = append(sets, setSql)
	}

	// 关键字段搜索条件处理
	referenceWhere = strings.TrimRight(referenceWhere, ",") + ")"

	// 更新sql合并when case sql
	for _,v := range sets {
		updateSql += v + ","
	}
	// 去掉最后一个逗号和加上主更新条件
	updateSql = strings.TrimRight(updateSql, ",") + referenceWhere

	// 处理共同更新条件
	subWhereSql := ""
	subWhereVal := make([]interface{}, 0)
	if shareWhereField != nil && len(shareWhereField) != 0 {
		for _, whereVal := range shareWhereField {
			for field, fiedlVal := range whereVal {
				subWhereSql += " and "
				field = strings.TrimSpace(field)
				whereArr := strings.Split(field, " ")
				if len(whereArr)  == 2 {
					subWhereSql += whereArr[0] + " " + whereArr[1] + " ?"
					subWhereVal = append(subWhereVal, fiedlVal)
				} else {
					switch fiedlVal.(type) {
					case string:
						subWhereSql += whereArr[0] + " = ?"
						subWhereVal = append(subWhereVal, fiedlVal)
					case []string:
						subWhereSql += whereArr[0] + " in("
						for _,v := range fiedlVal.([]string) {
							subWhereSql += "?,"
							subWhereVal = append(subWhereVal, v)
						}
						subWhereSql += ")"
					}
				}
			}
		}
	}

	// fmt.Println(updateSql + subWhereSql, updateValue, referenceValue, subWhereVal)

	// 合并预处理值
	updateValue = append(updateValue, referenceValue...)
	execBinding := append(updateValue, subWhereVal...)

	// 合并执行sql
	execSql := updateSql + subWhereSql
	// fmt.Println(execSql, execBinding)
	return d.Exec(execSql, execBinding...)
}


type EasyOrm struct {
	Host string
	Db string
	Port string
	Account string
	Password string
	Charset string
	Config gorm.Config
}

// 连接数据库
func (o *EasyOrm) Open() (*EasyDb, error) {
	dsn := o.Account + ":" + o.Password + "@tcp(" + o.Host + ":" + o.Port + ")/" + o.Db + "?charset=" + o.Charset +"&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	sqlDb, err := db.DB()
	if err != nil {
		return nil, err
	}
	easyDb := &EasyDb{sqlDb}
	if err != nil {
		return nil, err
	}

	return easyDb, nil
}

func main() {
	easy := &EasyOrm{
		Host:"192.168.78.135",
		Db:"test",
		Port:"3306",
		Account:"golang",
		Password:"golang",
		Charset:"utf8mb4",
		Config:gorm.Config{},
	}
	db, err := easy.Open()
	if err != nil {
		fmt.Println(err)
	}
	defer db.Close()

	multipleData := []map[string]string{
		map[string]string{"bid":"10000200275", "videoUrl":"xxxxx","videoPic":"yyyyyyy"},
		map[string]string{"bid":"10000200117", "videoUrl":"aaaaaaa"},
		map[string]string{"bid":"10000200004", "videoUrl":"bbbbbbb"},
	}

	shareWhereField := []map[string]interface{}{
		// map[string]interface{}{"type1":[]string{"1","2"}},
		map[string]interface{}{"createTime >":"0"},
	}


	fmt.Println(db.BatchUpdate("cdb_hunter", multipleData, shareWhereField))
}