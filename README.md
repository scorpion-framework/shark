Shark
<img align="right" alt="Logo" width="100" src="https://i.imgur.com/ef3a5Ph.png">
=======

[![DUB Package](https://img.shields.io/dub/v/shark.svg)](https://code.dlang.org/packages/shark)
[![Build Status](https://travis-ci.org/scorpion-framework/shark.svg?branch=master)](https://travis-ci.org/scorpion-framework/shark)

Native connector for various databases.

Supports:

- PostreSQL

Work in progress:

- MySQL (and MariaDB)

```d
import shark;

class Test : Entity {

	override string tableName() {
		return "test";
	}
	
	@PrimaryKey
	@AutoIncrement
	Integer testId;
	
	@NotNull
	String a;
	
	Short b;

}

Database database = new PostgresqlDatabase("localhost");
database.connect("test", "postgres", "root");

database.dropIfExists("test");
database.init!Test();

Test test = new Test();
test.a = "test";
database.insert(test);
assert(test.testId == 1); // auto-increment of primary key

Test[] result = database.select!Test();
assert(result.length == 1);
assert(result[0].testId == 1);
assert(result[0].a == "test");
```
