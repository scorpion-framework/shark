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

	}

	// table with composite primary key
	static class Test3 : Test {

		@Id
		Integer id1;

		@Id
		String id2;

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

		test1 = database.selectOne!(["string"], Test1)("test");
		assert(test1.test == "test");

		database.drop("test");

		database.init!Test2();

		Test2 test2 = new Test2();
		test2.a = true;
		//test2.b = 12;
		test2.c = 13;
		test2.d = 14;
		test2.e = null;
		test2.f = .55f;
		test2.g = 7.34823e+10;
		test2.h = ';';
		test2.i = "test";
		test2.l = [0, 1, 2, 55];
		test2.m = "___________________";
		test2.n = [0, 0, 0, 0, 0, 0, 0];
		database.insert(test2);

		Test2[] test2s = database.select!Test2();
		assert(test2s.length == 1);
		test2 = test2s[0];
		assert(test2.a == true);
		//assert(test2.b == 12);
		assert(test2.c == 13);
		assert(test2.d == 14);
		assert(test2.e.isNull);
		assert(test2.f == .55f);
		assert(test2.g == 7.34823e+10);
		assert(test2.h == ';');
		assert(test2.i == "test");
		assert(test2.l == [0, 1, 2, 55]);
		assert(test2.m == "___________________");
		assert(test2.n == [0, 0, 0, 0, 0, 0, 0]);

		database.drop("test");

		database.init!Test3();

		Test3 test3 = new Test3();
		test3.id1 = 1;
		test3.id2 = "test";
		test3.value = int.max;
		database.insert(test3);

	}

}
