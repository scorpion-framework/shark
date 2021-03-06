﻿module shark.test;

unittest {

	import std.exception : assertThrown;

	import shark.clause;
	import shark.database;
	import shark.entity;
	import shark.sql;

	import shark.impl.mysql;
	import shark.impl.postgresql;

	static class Test : Entity {

		override string tableName() {
			return "test";
		}

	}

	static class Test0 : Test {

		@PrimaryKey
		@AutoIncrement
		Integer testId;

		@Name("string")
		@Length(10)
		String test;

	}

	static class Test1 : Test0 {

		@NotNull
		Integer a;

		@Unique
		Short b;

	}

	// table with all the types
	static class Test2 : Test {

		Bool a;

		//Byte b;

		Short c;

		Integer d;

		Long e;

		Float f;

		Double g;

		Char h;

		@Length(80)
		String i;

		@Length(80)
		Binary l;

		Clob m;

		Blob n;

		Date o;

		DateTime p;

		Time q;

	}

	// table with composite primary key
	static class Test3 : Test {

		@PrimaryKey
		Integer id1;

		@PrimaryKey
		String id2;

		uint value;

	}

	static class Test4 : Test {

		string str;

	}

	Database[] databases;

	Database mysql = new MysqlDatabase("localhost");
	mysql.connect("test", "root", "root");
	//databases ~= mysql;

	Database maria = new MysqlDatabase("localhost", 3307);
	maria.connect("test", "root", "root");
	//databases ~= maria;

	Database postgres = new PostgresqlDatabase("localhost");
	postgres.connect("test", "postgres", "root");
	databases ~= postgres;

	foreach(database ; databases) {

		database.dropIfExists("test");

		database.init!Test0(); // create
		database.init!Test1(); // alter

		assert(database.select!Test1().length == 0);

		Test1 test1 = new Test1();
		test1.test = "test";
		test1.a = 55;
		test1.b = -1;
		database.insert(test1);
		assert(test1.testId == 1);

		test1.testId = null;
		assertThrown!DatabaseException(database.insert(test1)); // `b` is unique

		test1.a = null;
		assertThrown!DatabaseException(database.insert(test1)); // `a` cannot be null

		test1.a = 44;
		test1.b = 1;
		database.insert(test1, false);

		test1.a = 33;
		test1.b = 6;
		database.insert(test1);

		assert(database.select!Test1().length == 3);

		test1 = new Test1();
		test1.test = "test";
		test1 = database.selectOne!(["string"], Test1)(Database.Select(Clause.Where(var("string").equals("test"))));
		assert(test1.test == "test");

		Test1[] test1s = database.select!Test1(Database.Select(Clause.Order("a")));
		assert(test1s[0].a == 33);
		assert(test1s[1].a == 44);
		assert(test1s[2].a == 55);

		test1s = database.select!Test1(Database.Select(Clause.Where(var("a").lessThan(40) & var("b").notEquals(0))));
		assert(test1s.length == 1);
		assert(test1s[0].a == 33);

		database.drop("test");
		database.init!Test2();

		Test2 test2 = new Test2();
		test2.a = true;
		//test2.b = 12;
		test2.c = 13;
		test2.d = -14;
		test2.e = null;
		test2.f = .55f;
		test2.g = 7.34823e+10;
		test2.h = ';';
		test2.i = "test";
		test2.l = [0, 1, 2, 55];
		test2.m = "___________________";
		test2.n = [0, 0, 0, 0, 0, 0, 0];
		test2.o = Date(2018, 12, 31);
		test2.p = DateTime(2019, 1, 1, 0, 27, 43);
		test2.q = Time(0, 36, 12);
		database.insert(test2);

		Test2[] test2s = database.select!Test2();
		assert(test2s.length == 1);
		test2 = test2s[0];
		assert(test2.a == true);
		//assert(test2.b == 12);
		assert(test2.c == 13);
		assert(test2.d == -14);
		assert(test2.e.isNull);
		assert(test2.f == .55f);
		assert(test2.g == 7.34823e+10);
		assert(test2.h == ';');
		assert(test2.i == "test");
		assert(test2.l == [0, 1, 2, 55]);
		assert(test2.m == "___________________");
		assert(test2.n == [0, 0, 0, 0, 0, 0, 0]);
		assert(test2.o == Date(2018, 12, 31));
		assert(test2.p == DateTime(2019, 1, 1, 0, 27, 43));
		assert(test2.q == Time(0, 36, 12));

		database.drop("test");
		database.init!Test3();

		Test3 test3 = new Test3();
		test3.id1 = 1;
		test3.id2 = "test";
		test3.value = int.max;
		database.insert(test3);

		test3.value = 12;
		database.update!"value"(test3);

		test3 = database.selectId!Test3(test3);
		assert(test3.id1 == 1);
		assert(test3.id2 == "test");
		assert(test3.value == 12);

		database.del(test3);
		assert(database.select!Test3().length == 0);

		database.drop("test");
		database.init!Test4();

		Test4 test4 = new Test4();
		test4.str = "'";
		database.insert(test4);
		test4.str = "');drop table test;--";
		database.insert(test4);

		database.close();

	}

}
