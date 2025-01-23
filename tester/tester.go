package main

import (
	"context"
	"depoq"
	"fmt"
)

type Param struct {
	AccountTypeID int `depoq:"account_type_id"`
}

type Response struct {
	ID string `depoq:"id"`
}

func main() {
	client := depoq.Init("depoq")

	var res = make([]map[string]interface{}, 0)

	param := Param{
		AccountTypeID: 1,
	}

	rs, err := client.Run("test").
		WithParam("account_type_id", 2).
		WithParams(param).
		WithPaging(depoq.NewPagingCursor(5, "bmV4dF8tLV8wMUpESzlEMTNSVzVDSFQzUDRWWEtHU0ZHNF8tLV8wMUpESzlEMTNSVzVDSFQzUDRWWEtHU0ZHNA==", "id")).
		WithSorting("-id").
		ScanMaps(&res).
		Execute(context.Background())
	if err != nil {
		panic(err)
	}

	// rs.
	fmt.Println("===> columns", rs.Columns)
	// fmt.Println("===> paging", rs.Paging.PagingCursor.Next)
	if rs.HasSQLResult() {
		fmt.Println("has sql result")
		fmt.Println(rs.SQLResult.LastInsertId())
	}

	var count int
	for _, r := range res {
		count++
		fmt.Println(count, r)
	}
}
