module shark.test;

unittest {

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

		String test;

	}

	static class Test1 : Test0 {

		@NotNull
		Integer a;

		@Unique
		Short b;

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

		database.init!Test0();
		database.init!Test1();

		//TODO insert test

		//database.drop("test");

	}

}
