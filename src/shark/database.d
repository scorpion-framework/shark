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

	public abstract void close();

	// INIT
	
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
	 *    @PrimaryKey
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
					static if(is(uda == PrimaryKey)) ret.primaryKeys ~= field.name;
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

		string tableName;
		Field[] fields;
		string[] primaryKeys;

		static struct Field {

			string name;
			uint type;
			size_t length = 0;
			bool nullable = true;
			bool unique = false;
			bool autoIncrement = false;
			string defaultValue;

		}

	}

	// SELECT

	public static struct Select {

		Where where;
		
		Order order;

		size_t limit = 0;

		static struct Where {



		}

		static class Order {



		}

	}

	public T[] select(string[] fields, T:Entity, E...)(E args, Select select=Select.init) {
		SelectInfo selectInfo;
		selectInfo.tableName = new T().tableName;
		selectInfo.fields = fields;
		Result result = selectImpl(selectInfo, select);
		T[] ret;
		foreach(row ; result.rows) {
			T entity = new T();
			result.apply(entity, row);
			ret ~= entity;
		}
		return ret;
	}

	public T[] select(T:Entity)(Select select=Select.init) {
		return this.select!([], T)("", select);
	}

	public T selectOne(string[] fields, T:Entity, E...)(E args, Select select=Select.init) {
		select.limit = 1;
		T[] ret = this.select!(fields, T, E)(args, select);
		if(ret.length) return ret[0];
		else return null;
	}

	protected abstract Result selectImpl(SelectInfo, Select);

	protected static struct SelectInfo {

		string tableName;
		string[] fields;

	}

	deprecated alias SelectResult = Result;

	// INSERT
	
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
		Result result = insertImpl(generateInsertInfo(entity, updateId));
		foreach(row ; result.rows) result.apply(entity, row);
	}

	private InsertInfo generateInsertInfo(T:Entity)(T entity, bool updateId) {
		InsertInfo ret;
		ret.tableName = entity.tableName;
		static foreach(immutable member ; getEntityMembers!T) {
			{
				static if(hasUDA!(__traits(getMember, T, member), PrimaryKey)) if(updateId) ret.primaryKeys ~= memberName!(T, member);
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

	protected abstract Result insertImpl(InsertInfo);

	protected static struct InsertInfo {

		string tableName;
		Field[] fields;
		string[] primaryKeys;

		static struct Field {

			string name;
			string value;

		}

	}

	// UPDATE
	
	public void update(string[] fields, string[] where, T:Entity)(T entity) {

	}
	
	public void update(string[] fields, T:Entity)(T entity) {
		return update!(fields, [getEntityId!T], T)(entity);
	}
	
	public void update(T:Entity)(T entity) {
		return update!(getEntityMembers!T, T)(entity);
	}

	// DROP

	public abstract void dropIfExists(string table);

	public abstract void drop(string table);

	// UTILS
	
	public static struct Result {
		
		size_t[string] columns; // position in the array of the column
		
		Row[][] rows;

		void apply(T)(ref T entity, Row[] row) {
			static foreach(immutable member ; getEntityMembers!T) {
				{
					auto ptr = memberName!(T, member) in columns;
					if(ptr) {
						auto v = row[*ptr];
						if(v is null) {
							static if(memberNullable!(typeof(__traits(getMember, T, member)))) mixin("entity." ~ member).nullify();
							else throw new DatabaseException("Could not nullify " ~ T.stringof ~ "." ~ member);
						} else {
							alias R = typeof(__traits(getMember, T, member));
							static if(is(R == Bool) || is(R == bool)) {
								auto value = cast(Result.RowImpl!bool)v;
							} else static if(is(R == Byte) || is(R == byte) || is(R == ubyte)) {
								auto value = cast(Result.RowImpl!byte)v;
							} else static if(is(R == Short) || is(R == short) || is(R == ushort)) {
								auto value = cast(Result.RowImpl!short)v;
							} else static if(is(R == Integer) || is(R == int) || is(R == uint)) {
								auto value = cast(Result.RowImpl!int)v;
							} else static if(is(R == Long) || is(R == long) || is(R == ulong)) {
								auto value = cast(Result.RowImpl!long)v;
							} else static if(is(R == Float) || is(R == float)) {
								auto value = cast(Result.RowImpl!float)v;
							} else static if(is(R == Double) || is(R == double)) {
								auto value = cast(Result.RowImpl!double)v;
							} else static if(is(R == Char) || is(R == char)) {
								auto value = cast(Result.RowImpl!char)v;
							} else static if(is(R == String) || is(R == Clob) || is(R == string)) {
								auto value = cast(Result.RowImpl!string)v;
							} else static if(is(R == Binary) || is(R == Blob) || is(R == ubyte[])) {
								auto value = cast(Result.RowImpl!(ubyte[]))v;
							}
							if(value is null) throw new DatabaseException("Could not cast " ~ row[*ptr].toString() ~ " to " ~ R.stringof);
							mixin("entity." ~ member) = value.value;
						}
					}
				}
			}
		}
		
		static class Row {
			
			static Row from(T)(T value) {
				RowImpl!T ret = new RowImpl!T();
				ret.value = value;
				return ret;
			}
			
		}
		
		static class RowImpl(T) : Row {
			
			T value;
			
			override string toString() {
				import std.conv;
				return value.to!string;
			}
			
		}
		
	}

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
		static if(is(T == Bool) || is(T == bool)) {
			return BOOL;
		} else static if(is(T == Byte) || is(T == byte) || is(T == ubyte)) {
			return BYTE;
		} else static if(is(T == Short) || is(T == short) || is(T == ushort)) {
			return SHORT;
		} else static if(is(T == Integer) || is(T == int) || is(T == uint)) {
			return INT;
		} else static if(is(T == Long) || is(T == long) || is(T == ulong)) {
			return LONG;
		} else static if(is(T == Float) || is(T == float)) {
			return FLOAT;
		} else static if(is(T == Double) || is(T == double)) {
			return DOUBLE;
		} else static if(is(T == Char) || is(T == char)) {
			return CHAR;
		} else static if(is(T == Clob)) {
			return CLOB;
		} else static if(is(T == Blob)) {
			return BLOB;
		} else static if(is(T == String) || is(T : string)) {
			return STRING;
		} else static if(is(T == Binary) || is(T : ubyte[])) {
			return BINARY;
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
