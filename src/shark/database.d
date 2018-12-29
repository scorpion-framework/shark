module shark.database;

import std.conv : to;
import std.string : join;
import std.traits : hasUDA, getUDAs;

import shark.entity;
import shark.util : toSnakeCase;

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
	
	public void connect(string db, string user, string password="") {
		_db = db;
		this.connectImpl(db, user, password);
	}
	
	/**
	 * Initializes an entity, either by creating it or updating
	 * its fields when it already exists.
	 * Example:
	 * ---
	 * class Test : Entity {
	 * 
	 *    override string tableName() {
	 *        return "test";
	 *    }
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
	 * 
	 * database.init!Test();
	 * ---
	 */
	public void init(T:Entity)() if(isValidEntity!T) {
		enum initInfo = generateInitInfo!T(); // generate at compile time
		initImpl(initInfo);
	}

	private static InitInfo generateInitInfo(T:Entity)() {
		InitInfo ret;
		ret.tableName = new T().tableName;
		static foreach(immutable member ; getEntityMembers!T) {
			{
				InitInfo.Field field;
				field.name = memberName!(T, member);
				field.type = memberType!(typeof(__traits(getMember, T, member)));
				field.nullable = memberNullable!(typeof(__traits(getMember, T, member)));
				foreach(uda ; __traits(getAttributes, __traits(getMember, T, member))) {
					static if(is(uda == Id)) ret.primaryKey = field.name;
					else static if(is(uda == AutoIncrement)) {
						field.autoIncrement = true;
						field.nullable = false;
					}
					else static if(is(uda == NotNull)) field.nullable = false;
					else static if(is(uda == Unique)) field.unique = true;
					else static if(is(typeof(uda) == Length)) field.length = uda.length;
				}
				ret.fields ~= field;
			}
		}
		return ret;
	}

	protected abstract void initImpl(InitInfo);

	protected static struct InitInfo {

		/**
		 * Name of the table from the entity's tableName property.
		 */
		string tableName;

		/**
		 * Fields of the table from the entity's variables.
		 */
		Field[] fields;

		/**
		 * If not null indicates which field is the primary key.
		 */
		string primaryKey;

		static struct Field {

			/**
			 * Name of the field, either from @Name or converted from
			 * the variable's name.
			 */
			string name;

			/**
			 * Type of the field, from the variable's type.
			 */
			Type type;

			/**
			 * Optional length of the field from the @Length attribute.
			 * Indicates the length when higher than 0.
			 */
			size_t length = 0;

			/**
			 * Indicates whether the type can be null, either from the
			 * type of the variable or the @NotNull attribute.
			 */
			bool nullable = true;

			/**
			 * Indicates whether the type is unique in the table's rows,
			 * from the @Unique attribute.
			 */
			bool unique = false;

			/**
			 * Indicates whether the type should be incremented each type
			 * a new row is inserted.
			 */
			bool autoIncrement = false;

			/**
			 * Indicates the defaultValue from the @Default attribute.
			 */
			string defaultValue;

		}

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

	}
	
	public void update(string[] fields, string[] where, T:Entity)(T entity) {

	}
	
	public void update(string[] fields, T:Entity)(T entity) {
		return update!(fields, [getEntityId!T], T)(entity);
	}
	
	public void update(T:Entity)(T entity) {
		return update!(getEntityMembers!T, T)(entity);
	}

	public abstract void dropIfExists(string table);

	public abstract void drop(string table);

	protected enum Type {

		BOOL,
		BYTE,
		SHORT,
		INT,
		LONG,
		FLOAT,
		DOUBLE,
		STRING,
		BINARY

	}

}

private string memberName(T:Entity, string member)() {
	static if(hasUDA!(__traits(getMember, T, member), Name)) {
		return getUDAs!(__traits(getMember, T, member), Name)[0].name;
	} else {
		return member.toSnakeCase();
	}
}

private string escape(T)(T value) {
	static if(is(T : String) || is(T : string)) {
		//TODO properly escape
		return "`" ~ value ~ "`";
	} else static if(is(T : Nullable!R, R)) {
		if(value.isNull) return "null";
		else return value.value.to!string;
	} else {
		return value.to!string;
	}
}

private Database.Type memberType(T)() {
	with(Database.Type) {
		static if(is(T : Bool) || is(T == bool)) {
			return BOOL;
		} else static if(is(T : Byte) || is(T == byte) || is(T == ubyte)) {
			return BYTE;
		} else static if(is(T : Short) || is(T == short) || is(T == ushort)) {
			return SHORT;
		} else static if(is(T : Integer) || is(T == int) || is(T == uint)) {
			return INT;
		} else static if(is(T : Long) || is(T == long) || is(T == ulong)) {
			return LONG;
		} else static if(is(T : Float) || is(T == float)) {
			return FLOAT;
		} else static if(is(T : Double) || is(T == double)) {
			return DOUBLE;
		} else static if(is(T : String) || is(T : string)) {
			return STRING;
		} else static if(is(T : Binary) || is(T : ubyte[])) {
			return BINARY;
		}
	}
}

private bool memberNullable(T)() {
	static if(is(T == bool) || is(T == byte) || is(T == ubyte) || is(T == short) || is(T == ushort) || is(T == int) || is(T == uint) || is(T == long) || is(T == ulong) || is(T == float) || is(T == double)) {
		return false;
	} else {
		return true;
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
	return idCount <= 1;
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

	public @property T errorCode() {
		return _errorCode;
	}

}

class ErrorCodesDatabaseException(T) : DatabaseException {

	private T[] _errors;

	public this(T[] errors, string file=__FILE__, size_t line=__LINE__) {
		string[] messages;
		foreach(error ; errors) {
			messages ~= error.msg;
		}
		super(messages.join(", "), file, line);
	}

	public @property T[] errors() {
		return _errors;
	}

}
