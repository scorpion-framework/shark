module shark;

public import shark.clause : Clause, var;
public import shark.database : Database, DatabaseException;
public import shark.entity : Entity, Bool, Byte, Short, Integer, Long, Float, Double, Char, String, Binary, Clob, Blob, Date, DateTime, Name, PrimaryKey, AutoIncrement, NotNull, Unique, Length;

public import shark.impl.mysql : MysqlDatabase;
public import shark.impl.postgresql : PostgresqlDatabase;
