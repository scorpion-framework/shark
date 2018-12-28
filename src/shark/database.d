module shark.database;

import std.conv : to;
import std.string : join;
import std.traits : hasUDA, getUDAs;

import shark.entity;

// debug
import std.stdio : writeln;

/**
 * Represents a generic database.
 */
class Database {

	private string _db;
	
	public @property string db() {
		return _db;
	}
	
	protected abstract void connectImpl(string db, string user, string password);
	
	public void connect(string db, string user="root", string password="") {
		_db = db;
		this.connectImpl(db, user, password);
	}

	//public abstract string query(string query);
	
	/**
	 * Initializes an entity, either by creating it or updating
	 * its fields when it already exists.
	 * Example:
	 * ---
	 * class Test : Entity {
	 *
	 *    @Id
	 *    @AutoIncrement
	 *    Long testId;
	 *
	 *    @NotNull
	 *    Integer a;
	 *
	 *    @Length(10)
	 *    String b;
	 *
	 * }
	 * ---
	 */
	public void init(T:Entity)() if(isValidEntity!T) {
		//TODO call `describe ${new Entity().tableName}`
		string[] fields;
		static foreach(immutable member ; getEntityMembers!T) {
			{
				string[] attr = [memberName!(T, member), typeName!(typeof(__traits(getMember, T, member)))];
				foreach(uda ; __traits(getAttributes, __traits(getMember, T, member))) {
					static if(is(typeof(uda) == Length)) attr[1] ~= "(" ~ uda.length.to!string ~ ")";
				}
				foreach(uda ; __traits(getAttributes, __traits(getMember, T, member))) {
					static if(is(uda == Id)) attr ~= "primary key";
					else static if(is(uda == AutoIncrement)) attr ~= "auto_increment";
					else static if(is(uda == NotNull)) attr ~= "not null";
					else static if(is(uda == Unique)) attr ~= "unique";
				}
				fields ~=  attr.join(" ");
			}
		}
		writeln("create table " ~ new T().tableName ~ " (" ~ fields.join(",") ~ ");");
	}
	
	public T select(T:Entity)(T entity) {
		
		return null;
	}
	
	public T select(T:Entity, E...)(T entity) if(args.length) {
	
		return null;
	}
	
	public R selectFields(R, T:Entity, E...)(T entity) {
		
		return R.init;
	}
	
	/**
	 * Inserts a new entity into the database.
	 * Example:
	 * ---
	 * Test test = new Test();
	 * test.a = 55;
	 * test.b = "Test";
	 * database.insert(test);
	 * assert(!test.testId.isNull);
	 * ---
	 */
	public void insert(T:Entity)(T entity) {
		string[] names;
		string[] values;
		static foreach(immutable member ; getEntityMembers!T()) {
			if(!mixin("entity." ~ member).isNull) {
				names ~= memberName!(T, member);
				values ~= escape(mixin("entity." ~ member));
			}
		}
		writeln("insert into " ~ entity.tableName ~ " (" ~ names.join(", ") ~ ") values (" ~ values.join(", ") ~ ");");
	}
	
	public void update(string[] fields, string[] where, T:Entity)(T entity) {
		string[] queries;
		static foreach(immutable field ; fields) {
			queries ~= memberName!(T, field) ~ "=" ~ escape(mixin("entity." ~ field));
		}
		string[] wheres;
		static foreach(immutable w ; where) {
			wheres ~= memberName!(T, w) ~ "=" ~ escape(mixin("entity." ~ w));
		}
		writeln("update " ~ entity.tableName ~ " set " ~ queries.join(", ") ~ " where " ~ wheres.join(" and ") ~ ";");
	}
	
	public void update(string[] fields, T:Entity)(T entity) {
		return update!(fields, [getEntityId!T], T)(entity);
	}
	
	public void update(T:Entity)(T entity) {
		return update!(getEntityMembers!T, T)(entity);
	}

}

private string memberName(T:Entity, string member)() {
	static if(hasUDA!(__traits(getMember, T, member), Name)) {
		return getUDAs!(__traits(getMember, T, member), Name)[0].name;
	} else {
		return member/*.snakeCase*/;
	}
}

private string escape(T)(T value) {
	static if(is(T : String) || is(T : string)) {
		//TODO properly escape
		return "`" ~ value ~ "`";
	} else static if(is(T : Nullable!R, R)) {
		if(value.isNull) return "null";
		else return value.value.to!string;
	} else static if(is(T : Bool) || is(T == bool) || is(T : Byte) || is(T == byte) || is(T == ubyte) || is(T : Short) || is(T == short) || is(T == ushort) || is(T : Integer) || is(T == int) || is(T == uint) || is(T : Long) || is(T == long) || is(T == ulong)) {
		return value.to!string;
	}
}

private string typeName(T)() {
	static if(is(T : String) || is(T : string)) {
		return "varchar";
	} else static if(is(T : Binary) || is(T : ubyte[])) {
		return "binary";
	} else static if(is(T : Bool) || is(T == bool)) {
		return "boolean";
	} else static if(is(T : Byte) || is(T == byte) || is(T == ubyte)) {
		return "tinyint";
	} else static if(is(T : Short) || is(T == short) || is(T == ushort)) {
		return"shortint";
	} else static if(is(T : Integer) || is(T == int) || is(T == uint)) {
		return "integer";
	} else static if(is(T : Long) || is(T == long) || is(T == ulong)) {
		return "bigint";
	}
}

private string[] getEntityMembers(T:Entity)() {
	string[] ret;
	foreach(immutable member ; __traits(allMembers, T)) {
		static if(!is(typeof(__traits(getMember, T, member)) == function) && __traits(compiles, mixin("new T()." ~ member ~ "=T." ~ member ~ ".init"))) {
			ret ~= member;
		}
	}
	return ret;
}

private bool isValidEntity(T:Entity)() {
	size_t idCount = 0;
	static foreach(immutable member ; getEntityMembers!T()) {
		foreach(immutable uda ; __traits(getAttributes, __traits(getMember, T, member))) {
			static if(is(uda == Id)) idCount++;
		}
	}
	return idCount == 1;
}

private string getEntityId(T:Entity)() {
	static foreach(immutable member ; getEntityMembers!T()) {
		static if(hasUDA!(__traits(getMember, T, member), Id)) return member;
	}
}

class DatabaseException : Exception {

	public this(string msg, string file=__FILE__, size_t line=__LINE__) {
		super(msg, file, line);
	}

}

class DatabaseConnectionException : DatabaseException {

	public this(string msg, string file=__FILE__, size_t line=__LINE__) {
		super(msg, file, line);
	}

}

class ErrorCodeDatabaseException(string dbname, T) : DatabaseException {

	private T _errorCode;

	public this(T errorCode, string msg, string file=__FILE__, size_t line=__LINE__) {
		super("(" ~ dbname ~ "-" ~ errorCode.to!string ~ ") " ~ msg, file, line);
		_errorCode = errorCode;
	}

}
