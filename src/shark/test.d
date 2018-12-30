module shark.test;

unittest {

	import std.exception : assertThrown;

	import shark;

	static class Test : Entity {

		override string tableName() {
			return "test";
		}

	}

	static class Test0 : Test {

		@Id
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

		Byte b;

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

	}

	// table with composite primary key
	static class Test3 : Test {

		@Id
		Integer id1;

		@Id
		Integer id2;

		uint value;

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

		Test1 test1 = new Test1();
		test1.test = "test";
		test1.a = 55;
		test1.b = -1;
		database.insert(test1);
		assertThrown!DatabaseException(database.insert(test1)); // variable `b` should be unique
		test1.a = null;
		test1.b = 0;
		assertThrown!DatabaseException(database.insert(test1)); // variable `a` cannot be null

		test1.a = 44;
		test1.b = 1;
		database.insert(test1);

		test1.a = 33;
		test1.b = 6;
		database.insert(test1);

		test1 = database.selectOne!(["test"], Test1)("test");

		database.drop("test");

		database.init!Test2();

		Test2 test2 = new Test2();
		test2.a = true;
		test2.b = 12;
		test2.c = 13;
		test2.d = 14;
		test2.e = 15;
		test2.f = .55;
		test2.g = 10934871267.1;
		test2.h = ';';
		test2.i = "test";
		test2.l = [0, 1, 2, 3];
		test2.m = "___________________";
		test2.n = [0, 0, 0, 0, 0, 0, 0];
		database.insert(test2);

		database.drop("test");

		database.init!Test3();

		Test3 test3 = new Test3();
		test3.id1 = 1;
		test3.id2 = 109;
		test3.value = int.max;
		database.insert(test3);

	}

}
