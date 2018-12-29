module shark;

public import shark.database : Database, DatabaseException;
public import shark.entity : Entity, Bool, Byte, Short, Integer, Long, String, Binary, Id, AutoIncrement, NotNull, Unique;

public import shark.impl.mysql : MysqlDatabase;
public import shark.impl.postgresql : PostgresqlDatabase;
