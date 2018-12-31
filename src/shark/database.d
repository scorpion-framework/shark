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

	private string _db = null;

	/**
	 * Performs authentication and connect to a database.
	 * It is possible to reconnect using the same object by calling
	 * `close` and then `connect` again.
	 */
	public void connect(string db, string user, string password="") {
		_db = db;
		this.connectImpl(db, user, password);
	}

	/// ditto
	public void connect(string password="") {
		this.connect(null, "", password);
	}
	
	protected abstract void connectImpl(string db, string user, string password);

	/**
	 * Indicates the name of the database opened or null
	 * if the database isn't connected.
	 */
	public @property string db() {
		return _db;
	}

	/**
	 * Closes the connection with the database.
	 * Should only be called after `connect`.
	 */
	public void close() {
		closeImpl();
		_db = null;
	}

	protected abstract void closeImpl();

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

	/**
	 * Clauses for select.
	 * It is possible to instantiate the struct with the parameters
	 * in any order.
	 */
	public static struct Select {

		Clause.Where where;
		
		Clause.Order order;

		Clause.Limit limit;

		// calling this because of a bug in the default constructor
		private void set(Clause.Where where, Clause.Order order, Clause.Limit limit) {
			this.where = where;
			this.order = order;
			this.limit = limit;
		}

		this(Clause.Where where, Clause.Order=Clause.Order.init, Clause.Limit=Clause.Limit.init) {
			set(where, order, limit);
		}

		this(Clause.Where where, Clause.Limit limit, Clause.Order=Clause.Order.init) {
			set(where, order, limit);
		}

		this(Clause.Order order, Clause.Where=Clause.Where.init, Clause.Limit=Clause.Limit.init) {
			set(where, order, limit);
		}

		this(Clause.Order order, Clause.Limit limit, Clause.Where=Clause.Where.init) {
			set(where, order, limit);
		}

		this(Clause.Limit limit, Clause.Where where, Clause.Order=Clause.Order.init) {
			set(where, order, limit);
		}

		this(Clause.Limit limit, Clause.Order order, Clause.Where=Clause.Where.init) {
			set(where, order, limit);
		}

	}

	/**
	 * Selects entities from the database using the optional given clauses.
	 * The name of the fields should correspond to the name of the entity's
	 * fields in the database, not to the ones in the D program.
	 * Example:
	 * ---
	 * database.select!(["a", "b"], Test)();
	 * database.select!("testId", Test)();
	 * database.select!Test(Database.Select(Database.Select.Limit(10)));
	 * ---
	 */
	public T[] select(string[] fields, T:Entity)(Select select=Select.init) {
		return selectImpl!(fields, T)(select);
	}

	/// ditto
	public T[] select(string field, T:Entity)(Select select=Select.init) {
		return selectImpl!([field], T)(select);
	}

	/// ditto
	public T[] select(T:Entity)(Select select=Select.init) {
		return selectImpl!([], T)(select);
	}

	private T[] selectImpl(string[] fields, T:Entity)(Select select=Select.init) {
		SelectInfo selectInfo;
		selectInfo.tableName = new T().tableName;
		//static foreach(field ; fields) selectInfo.fields ~= memberName!(T, field);
		selectInfo.fields = fields;
		return selectImpl(selectInfo, select).bind!T();
	}

	/**
	 * Selects one entity from the database.
	 */
	public T selectOne(string[] fields, T:Entity)(Select select=Select.init) {
		return selectOneImpl!(fields, T)(select);
	}

	/// ditto
	public T selectOne(string field, T:Entity)(Select select=Select.init) {
		return selectOneImpl!([field], T)(select);
	}

	/// ditto
	public T selectOne(T:Entity)(Select select=Select.init) {
		return selectOneImpl!([], T)(select);
	}

	private T selectOneImpl(string[] fields, T:Entity)(Select select=Select.init) {
		select.limit = Clause.Limit(1);
		T[] ret = this.select!(fields, T)(select);
		if(ret.length) return ret[0];
		else return null;
	}

	/**
	 * Selects an entity from its primary key(s).
	 */
	public T selectId(string[] fields, T:Entity)(T entity, Select select=Select.init) if(getEntityPrimaryKeys!T.length) {
		return selectIdImpl!(fields, T)(entity, select);
	}

	/// ditto
	public T selectId(string field, T:Entity)(T entity, Select select=Select.init) if(getEntityPrimaryKeys!T.length) {
		return selectIdImpl!([field], T)(entity, select);
	}

	/// ditto
	public T selectId(T:Entity)(T entity, Select select=Select.init) if(getEntityPrimaryKeys!T.length) {
		return selectIdImpl!([], T)(entity, select);
	}

	private T selectIdImpl(string[] fields, T:Entity)(T entity, Select select=Select.init) if(getEntityPrimaryKeys!T.length) {
		return selectOne!(fields, T)(Select(makeWhereFromPrimaryKeys(entity)));
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

	/**
	 * Updates one or more table fields.
	 * The given fields should correspond to the ones in the entity class,
	 * not to the ones in the database.
	 */
	public void update(string[] fields, T:Entity)(T entity, Clause.Where where) if(fields.length) {
		UpdateInfo updateInfo;
		updateInfo.tableName = entity.tableName;
		static foreach(field ; fields) {
			updateInfo.fields ~= UpdateInfo.Field(memberName!(T, field), escape(mixin("entity." ~ field)));
		}
		updateImpl(updateInfo, where);
	}

	/// ditto
	public void update(string field, T:Entity)(T entity, Clause.Where where) {
		return update!([field], T)(entity, where);
	}

	/**
	 * Updates a single row of a table searching by the entity's
	 * primary key(s).
	 */
	public void update(string[] fields, T:Entity)(T entity) if(getEntityPrimaryKeys!T.length) {
		update!(fields, T)(entity, makeWhereFromPrimaryKeys(entity));
	}

	/// ditto
	public void update(string field, T:Entity)(T entity) if(getEntityPrimaryKeys!T.length) {
		return update!([field], T)(entity);
	}

	protected abstract void updateImpl(UpdateInfo, Clause.Where);

	protected static struct UpdateInfo {

		string tableName;
		Field[] fields;

		static struct Field {

			string name;
			string value;

		}

	}

	// DELETE

	/**
	 * Deletes row from a table.
	 */
	public void del(string table, Clause.Where where) {
		deleteImpl(table, where);
	}

	/**
	 * Deletes zero or one row using the entity's primary
	 * key(s).
	 * Example:
	 * ---
	 * database.del(test);
	 * ---
	 */
	public void del(T:Entity)(T entity) if(getEntityPrimaryKeys!T.length) {
		del(entity.tableName, makeWhereFromPrimaryKeys(entity));
	}

	protected abstract void deleteImpl(string, Clause.Where);

	// DROP

	public abstract void dropIfExists(string table);

	public abstract void drop(string table);

	// UTILS

	private Clause.Where makeWhereFromPrimaryKeys(T)(T entity) if(getEntityPrimaryKeys!T.length) {
		Clause.Where where;
		Clause.Where.GenericStatement[] statements;
		static foreach(immutable member ; getEntityPrimaryKeys!T) {
			statements ~= new Clause.Where.Statement(memberName!(T, member), Clause.Where.Operator.equals, mixin("entity." ~ member));
		}
		where.statement = statements[0];
		foreach(statement ; statements[1..$]) {
			where.statement = new Clause.Where.ComplexStatement(where.statement, Clause.Where.Glue.and, statement);
		}
		return where;
	}

	/**
	 * Clauses for select, update and delete.
	 */
	public static struct Clause {

		@disable this();

		/**
		 * Where clause.
		 */
		static struct Where {

			static interface GenericStatement {}

			static class Statement : GenericStatement {

				string field;

				Operator operator;

				string value;

				bool needsEscaping;

				this(T)(string field, Operator operator, T value) {
					this.field = field;
					this.operator = operator;
					this.value = value.to!string;
					static if(is(T : string)) needsEscaping = true;
				}

				this(T)(string field, string operator, T value) {
					this(field, fromString(operator), value);
				}

				private Operator fromString(string operator) {
					switch(operator) with(Operator) {
						case "=": case "==": return equals;
						case "!=": return notEquals;
						case ">": return greaterThan;
						case ">=": return greaterThanOrEquals;
						case "<": return lessThan;
						case "<=": return lessThanOrEquals;
						default: throw new Exception("Unknown operator " ~ operator);
					}
				}

			}

			static class ComplexStatement : GenericStatement {

				GenericStatement leftStatement;

				Glue glue;

				GenericStatement rightStatement;

				this(GenericStatement leftStatement, Glue glue, GenericStatement rightStatement) {
					this.leftStatement = leftStatement;
					this.glue = glue;
					this.rightStatement = rightStatement;
				}

			}

			enum Operator {

				isNull,
				equals,
				notEquals,
				greaterThan,
				greaterThanOrEquals,
				lessThan,
				lessThanOrEquals,

			}

			enum Glue {

				and,
				or

			}
			
			GenericStatement statement;

			/+/**
			 * Constructs a where clause from a string.
			 * Note that this function does not parse a SQL where clause
			 * but a proprietary one.
			 */
			static Where fromString(E...)(string str, E args) {
				//import std.regex : ctRegex;
				//enum regex = ctRegex!`\(([^\(\)]*)\)`;
				Statement[] ret;

				return Where(ret);
			}

			///
			unittest {

				Where.fromString("a > 50");
				Where.fromString("a == b");
				Where.fromString("(a > 50 or b < 50) and c == 'test'");
				Where.fromString("a > b and (c is null or (c >= d or e != f))");

			}+/
			
		}

		/**
		 * Order clause.
		 * Example:
		 * ---
		 * Order(Order.Field("a", Order.Field.desc), Order.Field("b", Order.Field.asc));
		 * ---
		 */
		static struct Order {

			enum random = { Order order; order.rand=true; return order; }();

			bool rand = false;

			Field[] fields;

			this(Field[] fields...) {
				this.fields = fields;
			}

			this(string[] fields...) {
				foreach(field ; fields) this.fields ~= Field(field);
			}

			static struct Field {

				enum asc = true;
				enum desc = false;

				string name;

				bool _asc = true;

			}
			
		}

		/**
		 * Indicates the limit of rows to be returned. It can be single
		 * using the 1-field constructor or complex (lower and upper limit)
		 * using the 2-field constrcutor.
		 */
		static struct Limit {
			
			size_t lower, upper;
			
			this(size_t lower, size_t upper) {
				assert(lower < upper);
				this.lower = lower;
				this.upper = upper;
			}
			
			this(size_t limit) {
				this(0, limit);
			}
			
		}

	}

	/**
	 * Result of a select query.
	 */
	public static struct Result {
		
		size_t[string] columns; // position in the array of the column
		
		Row[][] rows;

		/**
		 * Creates n objects from the result. T doesn't have to
		 * extend `Entity`.
		 * Example:
		 * ---
		 * class Test {
		 * 
		 *    String a;
		 * 
		 *    Integer b;
		 * 
		 * }
		 * ...
		 * Test[] entities = result.bind!Test();
		 * ---
		 */
		public T[] bind(T)() {
			T[] ret;
			foreach(row ; rows) {
				T entity;
				static if(is(T == class)) entity = new T();
				apply(entity, row);
				ret ~= entity;
			}
			return ret;
		}

		/**
		 * Applies the result of one row to entity, passed by reference.
		 * The entitty doesn't have to extend `Entity`.
		 */
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

/**
 * Indicates whether an entity is valid.
 * A valid entity extends `Entity`, is not abstract, has an
 * empty constructor and no duplicated members.
 */
public bool isValidEntity(T)() {
	static if(!is(T : Entity) || !__traits(compiles, new T())) {
		return false;
	} else {
		import std.algorithm : sort, uniq;
		import std.array : array;
		string[] members;
		static foreach(immutable member ; getEntityMembers!T) members ~= memberName!(T, member);
		sort(members);
		return uniq(members).array.length == members.length;
	}
}

///
unittest {

	static struct Invalid0 {}

	static class Invalid1 {}

	static abstract class Invalid2 {}

	static class Invalid3 : Entity {

		override string tableName() { return "test"; }

		this(int i) {}

	}

	static class Invalid4 : Entity {
		
		override string tableName() { return "test"; }

		String test;

		@Name("test")
		String test0;

	}

	static assert(!isValidEntity!Invalid0);
	static assert(!isValidEntity!Invalid1);
	static assert(!isValidEntity!Invalid2);
	static assert(!isValidEntity!Invalid3);
	static assert(!isValidEntity!Invalid4);

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
		} else {
			static assert(0, "Member of type " ~ T.stringof ~ " is not valid");
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

private string[] getEntityPrimaryKeys(T:Entity)() {
	string[] ret;
	static foreach(immutable member ; getEntityMembers!T) {
		static if(hasUDA!(__traits(getMember, T, member), PrimaryKey)) ret ~= member;
	}
	return ret;
}

/**
 * Generic database exception.
 */
class DatabaseException : Exception {

	public this(string msg, string file=__FILE__, size_t line=__LINE__) {
		super(msg, file, line);
	}

}

/**
 * Exception thrown when an error occurs during the connection,
 * like an unexpected or malformed packet.
 */
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
