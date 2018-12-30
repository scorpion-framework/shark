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

	public void connect(string password="") {
		this.connect(null, "", password);
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
	public void init(T:Entity)() {
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
					static if(is(uda == Id)) ret.primaryKeys ~= field.name;
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
		string[] primaryKeys;

		static struct Field {

			/**
			 * Name of the field, either from @Name or converted from
			 * the variable's name.
			 */
			string name;

			/**
			 * Type of the field, from the variable's type.
			 */
			uint type;

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

	public static struct Select {

		Where where;
		
		Order order;

		size_t limit = 0;

		static struct Where {



		}

		static class Order {



		}

	}

	public T[] select(string[] fields, T:Entity, E...)(E args, Select select=Select.init) if(args.length == fields.length) {
		SelectInfo selectInfo;
		selectInfo.tableName = new T().tableName;
		selectImpl(selectInfo, select);
		return [];
	}

	public T selectOne(string[] fields, T:Entity, E...)(E args, Select select=Select.init) {
		select.limit = 1;
		T[] ret = this.select!(fields, T, E)(args, select);
		if(ret.length) return ret[0];
		else return null;
	}

	protected abstract void selectImpl(SelectInfo, Select);

	protected static struct SelectInfo {

		string tableName;

	}
	
	/**
	 * Inserts a new entity into the database.
	 * This method does not alter the entity: to update it after an insert
	 * use select.
	 * Example:
	 * ---
	 * Test test = new Test();
	 * test.a = 55;
	 * test.b = "Test";
	 * database.insert(test);
	 * ---
	 */
	public void insert(T:Entity)(T entity, bool updateId=true) {
		insertImpl(generateInsertInfo(entity));

	}

	private InsertInfo generateInsertInfo(T:Entity)(T entity) {
		InsertInfo ret;
		ret.tableName = entity.tableName;
		static foreach(immutable member ; getEntityMembers!T) {
			{
				static if(!memberNullable!(typeof(__traits(getMember, T, member)))) enum condition = "true";
				else enum condition = "!entity." ~ member ~ ".isNull";
				if(mixin(condition)) {
					InsertInfo.Field field;
					field.name = memberName!(T, member);
					field.value = escape(mixin("entity." ~ member));
					ret.fields ~= field;
				}
			}
		}
		return ret;
	}

	protected abstract void insertImpl(InsertInfo);

	protected static struct InsertInfo {

		/**
		 * Name of the table from the entity's tableName property.
		 */
		string tableName;

		/**
		 * Fields of the table from the entity's variables.
		 */
		Field[] fields;

		static struct Field {

			/**
			 * Name of the field, either from @Name or converted from
			 * the variable's name.
			 */
			string name;

			string value;

		}

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

	protected enum Type : uint {

		BOOL = 1 << 0,
		BYTE = 1 << 1,
		SHORT = 1 << 2,
		INT = 1 << 3,
		LONG = 1 << 4,
		FLOAT = 1 << 5,
		DOUBLE = 1 << 6,
		CHAR = 1 << 7,
		STRING = 1 << 8,
		BINARY = 1 << 9,
		CLOB = 1 << 10,
		BLOB = 1 << 11,

	}
	
	protected string escape(T)(T value) {
		static if(is(T == Char) || is(T == char)) {
			return escapeString([value]);
		} else static if(is(T == String) || is(T == Clob) || is(T : string)) {
			return escapeString(value);
		} else static if(is(T == Binary) || is(T == Blob) || is(T : ubyte[])) {
			return escapeBinary(value);
		} else static if(is(T : Nullable!R, R)) {
			if(value.isNull) return "null";
			else return value.value.to!string;
		} else {
			return value.to!string;
		}
	}

	protected abstract string escapeString(string);

	protected abstract string escapeBinary(ubyte[]);

}

private string memberName(T:Entity, string member)() {
	static if(hasUDA!(__traits(getMember, T, member), Name)) {
		return getUDAs!(__traits(getMember, T, member), Name)[0].name;
	} else {
		return member.toSnakeCase();
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
		} else static if(is(T : Char) || is(T == char)) {
			return CHAR;
		} else static if(is(T : String) || is(T : string)) {
			return STRING;
		} else static if(is(T : Binary) || is(T : ubyte[])) {
			return BINARY;
		} else static if(is(T == Clob)) {
			return CLOB;
		} else static if(is(T == Blob)) {
			return BLOB;
		}
	}
}

private bool memberNullable(T)() {
	static if(is(T : Nullable!R, R)) return true;
	else return false;
	/*static if(is(T == bool) || is(T == byte) || is(T == ubyte) || is(T == short) || is(T == ushort) || is(T == int) || is(T == uint) || is(T == long) || is(T == ulong) || is(T == float) || is(T == double) || is(T == char)) {
		return false;
	} else {
		return true;
	}*/
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
