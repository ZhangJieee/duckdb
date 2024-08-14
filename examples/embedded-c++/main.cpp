#include "duckdb.hpp"
#include <iostream>
#include <string.h>
#include<unistd.h>

using namespace duckdb;

int main() {
	DuckDB db(":disk:");

	Connection con(db);

//	std::cout << " ---------------------- Create ----------------------" << std::endl;
//	con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY, j INTEGER)");

//	std::cout << " ---------------------- Write ----------------------" << std::endl;
//	auto result = con.Query("INSERT INTO integers VALUES (10, 20)");
//	con.Query("INSERT INTO integers VALUES (20, 30)");
//	con.Query("INSERT INTO integers VALUES (30, 40)");
//	std::cout << " ---------------------- Read ----------------------" << std::endl;
//	auto result = con.Query("SELECT * FROM integers");

//	std::cout << " ---------------------- Update ----------------------" << std::endl;
//	auto result = con.Query("UPDATE integers_1 SET j = 50 WHERE k >= 30");

//	std::cout << " ---------------------- Create ----------------------" << std::endl;
//	con.Query("CREATE TABLE integers_1(i INTEGER PRIMARY KEY, j INTEGER, k INTEGER)");
//
//	std::cout << " ---------------------- Write ----------------------" << std::endl;
//	auto result = con.Query("INSERT INTO integers_1 VALUES (10, 20, 30)");
//	con.Query("INSERT INTO integers_1 VALUES (20, 30, 40)");
//	con.Query("INSERT INTO integers_1 VALUES (30, 40, 50)");

	/*
	 * TEST for sort
	 * CREATE TABLE test(a INTEGER, b VARCHAR, c INTEGER, d VARCHAR)
	 * INSERT INTO test VALUES (1, 'a', 2, 'b'), (3, 'c', 4, 'd'), (10, 'e', 15, 'u'), (6, 'h', 9, 'z'), (26, 'o', 8, 'q')
	 * SELECT * from test order by a, b desc, c, d desc
	 * */

	/*
	 * TEST for group by
	 * CREATE TABLE test_group(sch VARCHAR, clas INTEGER, stu VARCHAR, grade INTEGER)
	 * INSERT INTO test VALUES ('chengxi', 141, 'qa', 10), ('chengxi', 141, 'zx', 23), ('chengxi', 141, 'ws', 5), ('chengxi', 141,'cv', 78), ('chengxi', 141, 'de', 90)
	 * SELECT sch, clas, avg(grade) from test_group group by sch, clas
	 * */

//	con.Query("CREATE TABLE test_group(sch VARCHAR, clas INTEGER, stu VARCHAR, grade INTEGER)");
//	auto result = con.Query("INSERT INTO test_group VALUES ('chengxi', 141, 'qa', 10), ('chengxi', 141, 'zx', 23), ('chengxi', 141, 'ws', 5), ('chengxi', 141,'cv', 78), ('chengxi', 141, 'de', 90)");
	std::cout << " ---------------------- Read ----------------------" << std::endl;
//	auto result = con.Query("SELECT * FROM integers as l join integers_1 as r on l.i = r.i where l.j > 0 and r.k = 40");
	auto result = con.Query("SELECT sch, clas, avg(grade), sum(grade) from test_group as g,test as t where g.grade = t.a group by sch, clas");
//	auto result = con.Query("CREATE UNIQUE INDEX k_idx ON integers_1 (k);");
//	auto result = con.Query("SELECT * from test where a in (select j from integers_1 where i > 1)");
	result->Print();
	return 0;
}
