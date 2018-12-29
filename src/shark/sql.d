module shark.sql;

import std.exception : enforce;
import std.string : join;

import shark.database;

import xbuffer : Buffer;

// debug
import std.stdio;

abstract class SqlDatabase : Database {

	public abstract Buffer query(string);

	// TABLE CREATION AND ALTERATION

	protected override void initImpl(InitInfo initInfo) {
		TableInfo[string] tableInfo = getTableInfo(initInfo.tableName);
		if(tableInfo is null) {
			//create the table
			string[] fields;
			foreach(field ; initInfo.fields) {
				fields ~= generateField(field);
			}
			if(initInfo.primaryKey.length) {
				fields ~= "primary key(" ~ initInfo.primaryKey ~ ")";
			}
			query("create table " ~ initInfo.tableName ~ " (" ~ fields.join(",") ~ ");");
		} else {
			// alter the table
			foreach(field ; initInfo.fields) {
				auto ptr = field.name in tableInfo;
				if(ptr) {
					// compare
					//enforce!DatabaseException(field.type == ptr.type, "Type cannot be changed!");
					if(field.type != ptr.type || field.nullable != ptr.nullable) {
						writeln("Field ", field.name, " was changed from ", ptr.nullable, " to ", field.nullable);
						alterTableColumn(initInfo.tableName, field, field.type != ptr.type, field.nullable != ptr.nullable);
					}
				} else {
					// field added
					query("alter table " ~ initInfo.tableName ~ " add " ~ generateField(field) ~ ";");
				}
				tableInfo.remove(field.name);
			}
			foreach(name, field; tableInfo) {
				// field removed, just drop it
				query("alter table " ~ initInfo.tableName ~ " drop " ~ name ~ ";");
			}
		}
	}

	/**
	 * Returns: table info or null if the table doesn't exists.
	 */
	protected abstract TableInfo[string] getTableInfo(string table);

	protected static struct TableInfo {

		string name;

		Type type;

		size_t length;

		bool nullable;

		string defaultValue = null;

	}

	protected abstract string generateField(InitInfo.Field field);

	protected abstract void alterTableColumn(string table, InitInfo.Field field, bool typeChanged, bool nullableChanged);

	// DROPPING

	public override void dropIfExists(string table) {
		query("drop table if exists " ~ table ~ ";");
	}

	public override void drop(string table) {
		query("drop table " ~ table ~ ";");
	}

}
