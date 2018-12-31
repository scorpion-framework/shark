module shark.sql;

import std.conv : to;
import std.exception : enforce;
import std.experimental.logger : warning;
import std.string : join;

import shark.database;
import shark.entity;

import xbuffer : Buffer;

// debug
import std.stdio;

/**
 * Generic SQL database. It is possible to execute queries and
 * select queries.
 * See specific implementations for more complex operations.
 */
abstract class SqlDatabase : Database {

	/**
	 * Runs a query without receiving anything back.
	 * Note that running just this method may break some implementations.
	 * Example:
	 * ---
	 * database.query("drop table test;");
	 * ---
	 */
	public abstract void query(string);

	/**
	 * Runs a select query and returns the result. This method
	 * does not break the flow of the protocol like `query` does.
	 * This method is intended for usage with complex queries.
	 * Example:
	 * ---
	 * auto result = database.querySelect("select * from test order by rand() limit 1");
	 * result.bind!Test();
	 * ---
	 */
	public abstract Result querySelect(string);

	// CREATE | ALTER

	protected override void initImpl(InitInfo initInfo) {
		TableInfo[string] tableInfo = getTableInfo(initInfo.tableName);
		if(tableInfo is null) {
			//create the table
			string[] fields;
			foreach(field ; initInfo.fields) {
				fields ~= generateField(field);
			}
			if(initInfo.primaryKeys.length) {
				fields ~= "primary key(" ~ initInfo.primaryKeys.join(",") ~ ")";
			}
			createTable(initInfo.tableName, fields);
		} else {
			// alter the table
			foreach(field ; initInfo.fields) {
				auto ptr = field.name in tableInfo;
				if(ptr) {
					// compare
					//enforce!DatabaseException(field.type == ptr.type, "Type cannot be changed!");
					if((field.type & ptr.type) == 0 || field.nullable != ptr.nullable) {
						alterTableColumn(initInfo.tableName, field, (field.type & ptr.type) == 0, field.nullable != ptr.nullable);
					}
				} else {
					// field added
					alterTableAddColumn(initInfo.tableName, field);
				}
				tableInfo.remove(field.name);
			}
			foreach(name, field; tableInfo) {
				// field removed, just drop it
				alterTableDropColumn(initInfo.tableName, name);
			}
		}
	}

	/**
	 * Returns: table info or null if the table doesn't exists.
	 */
	protected abstract TableInfo[string] getTableInfo(string table);

	protected static struct TableInfo {

		string name;

		uint type;

		size_t length;

		bool nullable;

		string defaultValue = null;

	}

	protected abstract string generateField(InitInfo.Field field);

	protected void createTable(string table, string[] fields) {
		query("create table " ~ table ~ " (" ~ fields.join(",") ~ ");");
	}

	protected abstract void alterTableColumn(string table, InitInfo.Field field, bool typeChanged, bool nullableChanged);

	protected void alterTableAddColumn(string table, InitInfo.Field field) {
		query("alter table " ~ table ~ " add " ~ generateField(field) ~ ";");
	}

	protected void alterTableDropColumn(string table, string column) {
		query("alter table " ~ table ~ " drop " ~ column ~ ";");
	}

	// SELECT

	protected override Result selectImpl(SelectInfo selectInfo, Select select) {
		string where;
		string[] order;
		if(select.where.statement !is null) {
			where = stringifyStatements(select.where.statement);
		}
		if(select.order.rand) {
			order ~= randomFunction;
		} else if(select.order.fields.length) {
			foreach(field ; select.order.fields) {
				order ~= field.name ~ " " ~ (field.asc ? "asc" : "desc");
			}
		}
		string q = "select " ~ (selectInfo.fields.length ? selectInfo.fields.join(",") : "*") ~ " from " ~ selectInfo.tableName;
		if(where.length) q ~= " where " ~ where;
		if(order.length) q ~= " order by " ~ order.join(",");
		if(select.limit.upper != 0) {
			if(select.limit.lower == 0) q ~= " limit " ~ select.limit.upper.to!string;
			else q ~= " limit " ~ select.limit.lower.to!string ~ "," ~ select.limit.upper.to!string;
		}
		return querySelect(q ~ ";");
	}

	// INSERT

	protected override Result insertImpl(InsertInfo insertInfo) {
		string[] names;
		string[] values;
		foreach(field ; insertInfo.fields) {
			names ~= field.name;
			values ~= field.value;
		}
		return insertInto(insertInfo.tableName, names, values, insertInfo.primaryKeys);
	}

	protected abstract Result insertInto(string table, string[] names, string[] fields, string[] primaryKeys);

	// UPDATE

	protected override void updateImpl(UpdateInfo updateInfo, Clause.Where where) {
		string[] sets;
		foreach(field ; updateInfo.fields) {
			sets ~= field.name ~ "=" ~ field.value;
		}
		string q = "update " ~ updateInfo.tableName ~ " set " ~ sets.join(",");
		if(where.statement !is null) q ~= " where " ~ stringifyStatements(where.statement);
		else warning("Where statement is empty! Updating the whole table!");
		query(q ~ ";");
	}

	// DELETE

	protected override void deleteImpl(string table, Clause.Where where) {
		string q = "delete from " ~ table;
		if(where.statement !is null) q ~= " where " ~ stringifyStatements(where.statement);
		else warning("Where statement is empty! Deleting the whole table!");
		query(q ~ ";");
	}

	// DROP

	public override void dropIfExists(string table) {
		query("drop table if exists " ~ table ~ ";");
	}

	public override void drop(string table) {
		query("drop table " ~ table ~ ";");
	}

	// UTILS

	protected string stringifyStatements(Clause.Where.GenericStatement statement) {
		auto complex = cast(Clause.Where.ComplexStatement)statement;
		if(complex) {
			return "(" ~ stringifyStatements(complex.leftStatement) ~ ") " ~ glueToString(complex.glue) ~ " (" ~ stringifyStatements(complex.rightStatement) ~ ")";
		} else {
			auto simple = cast(Clause.Where.Statement)statement;
			assert(simple !is null);
			if(simple.needsEscaping) return simple.field ~ " " ~ operatorToString(simple.operator) ~ " " ~ escape(simple.value);
			else return simple.field ~ " " ~ operatorToString(simple.operator) ~ " " ~ simple.value;
		}
	}

	protected string operatorToString(Clause.Where.Operator operator) {
		final switch(operator) with(Clause.Where.Operator) {
			case isNull: return "is";
			case equals: return "=";
			case notEquals: return "!=";
			case greaterThan: return ">";
			case greaterThanOrEquals: return ">=";
			case lessThan: return "<";
			case lessThanOrEquals: return "<=";
		}
	}

	protected string glueToString(Clause.Where.Glue glue) {
		final switch(glue) with(Clause.Where.Glue) {
			case or: return "or";
			case and: return "and";
		}
	}
	
	protected abstract @property string randomFunction();

	protected override string escapeString(string value) {
		import std.string : replace;
		return "'" ~ value.replace("'", "''") ~ "'";
	}

	/**
	 * Utilities for prepared statements.
	 */
	public static struct Prepared {

		static interface Param {

			public @property Type type();

		}

		static class ParamImpl(T, Type _type) : Param {

			public T value;

			public override Type type() {
				return _type;
			}

			public this(T value) {
				this.value = value;
			}

			override string toString() {
				import std.conv : to;
				return value.to!string;
			}

			alias value this;

		}

		static Param[] prepare(E...)(E params) {
			Param[] ret;
			foreach(param ; params) {
				alias T = typeof(param);
				static if(is(T == Bool) || is(T == bool)) ret ~= new ParamImpl!(bool, Type.BOOL)(param);
				else static if(is(T == Byte) || is(T == byte) || is(T == ubyte)) ret ~= new ParamImpl!(byte, Type.BYTE)(param);
				else static if(is(T == Short) || is(T == short) || is(T == ushort)) ret ~= new ParamImpl!(short, Type.SHORT)(param);
				else static if(is(T == Integer) || is(T == int) || is(T == uint)) ret ~= new ParamImpl!(int, Type.INT)(param);
				else static if(is(T == Long) || is(T == long) || is(T == ulong)) ret ~= new ParamImpl!(long, Type.LONG)(param);
				// ...
				else static if(is(T == String) || is(T == string)) ret ~= new ParamImpl!(string, Type.STRING)(param);
				else static assert(0, "Type " ~ T.stringof ~ " not supported");
			}
			return ret;
		}

	}

}
