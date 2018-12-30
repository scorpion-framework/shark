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
database.init!Test();

Test test = new Test();
test.a = "test";
database.insert(test);
```
