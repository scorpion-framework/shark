module shark.sql;

import std.exception : enforce;
import std.string : join;

import shark.database;

import xbuffer : Buffer;

// debug
import std.stdio;

abstract class SqlDatabase : Database {

	public abstract Buffer query(string);

	public abstract SelectResult querySelect(string);

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
			query("create table " ~ initInfo.tableName ~ " (" ~ fields.join(",") ~ ");");
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

	protected abstract void alterTableColumn(string table, InitInfo.Field field, bool typeChanged, bool nullableChanged);

	protected void alterTableAddColumn(string table, InitInfo.Field field) {
		query("alter table " ~ table ~ " add " ~ generateField(field) ~ ";");
	}

	protected void alterTableDropColumn(string table, string column) {
		query("alter table " ~ table ~ " drop " ~ column ~ ";");
	}

	// SELECT

	protected override SelectResult selectImpl(SelectInfo selectInfo, Select select) {
		return querySelect("select " ~ (selectInfo.fields.length ? selectInfo.fields.join(",") : "*") ~ " from " ~ selectInfo.tableName ~ ";");
	}

	// INSERT

	protected override void insertImpl(InsertInfo insertInfo) {
		string[] names;
		string[] values;
		foreach(field ; insertInfo.fields) {
			names ~= field.name;
			values ~= field.value;
		}
		query("insert into " ~ insertInfo.tableName ~ " (" ~ names.join(",") ~ ") values (" ~ values.join(",") ~ ");");
	}

	// DROP

	public override void dropIfExists(string table) {
		query("drop table if exists " ~ table ~ ";");
	}

	public override void drop(string table) {
		query("drop table " ~ table ~ ";");
	}

	// UTILS

	protected override string escapeString(string value) {
		//TODO properly escape
		return "'" ~ value ~ "'";
	}

}
