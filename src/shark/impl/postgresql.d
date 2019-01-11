module shark.impl.postgresql;

import std.algorithm : canFind;
import std.conv : to;
static import std.datetime;
import std.digest : toHexString, LetterCase;
import std.digest.md : md5Of;
import std.exception : enforce;
import std.experimental.logger : trace, info, warning;
import std.socket;
import std.string : join, replace;
import std.system : Endian;

import shark.clause;
import shark.database : DatabaseException, DatabaseConnectionException, ErrorCodeDatabaseException, ErrorCodesDatabaseException;
import shark.sql : SqlDatabase;
import shark.util : Stream, read0String, write0String, fromHexString;

import xbuffer : Buffer;

// debug
import std.stdio;

private enum infoStatement = "_shark_table_info";

private alias PostgresqlStream = Stream!(1, Endian.bigEndian, 4, true);

/**
 * PostgreSQL database implementation.
 */
class PostgresqlDatabase : SqlDatabase {

	private PostgresqlStream _stream;

	private string[string] _status;

	private uint _serverProcessId;
	private uint _serverSecretKey;

	private bool _error = false;

	public this(string host, ushort port=5432) {
		Socket socket = new TcpSocket();
		socket.blocking = true;
		socket.connect(getAddress(host, port)[0]);
		_stream = new PostgresqlStream(socket, 1024);
	}

	protected override void connectImpl(string db, string user, string password) {
		Buffer buffer = new Buffer(64);
		buffer.write!(Endian.bigEndian, uint)(0x0003_0000);
		buffer.write0String("user");
		buffer.write0String(user);
		buffer.write0String("database");
		buffer.write0String(db);
		buffer.write(ubyte(0));
		buffer.write!(Endian.bigEndian, uint)(buffer.data.length.to!uint + 4, 0);
		_stream.socket.send(buffer.data);
		buffer = receive();
		enforcePacketSequence('R');
		immutable method = buffer.read!(Endian.bigEndian, uint)();
		bool passwordRequired = true;
		string hashedPassword;
		switch(method) {
			case 0:
				// no password required
				passwordRequired = false;
				break;
			case 3:
				// plain text password
				hashedPassword = password;
				break;
			case 5:
				// hashed password (default)
				void[] salt = buffer.readData(4);
				hashedPassword = "md5" ~ toHexString!(LetterCase.lower)(md5Of(toHexString!(LetterCase.lower)(md5Of(password, user)), salt)).idup;
				break;
			default:
				throw new DatabaseConnectionException("Unknown authentication method requested by the server (" ~ method.to!string ~ ")");
		}
		if(passwordRequired) {
			buffer.reset();
			_stream.id = "p";
			buffer.write0String(hashedPassword);
			_stream.send(buffer);
			buffer = receive();
			enforcePacketSequence('R');
			enforce!DatabaseConnectionException(buffer.read!(Endian.bigEndian, uint)() == 0, "Authentication failed");
		}
		bool loop = true;
		do {
			buffer = receive();
			switch(_stream.id!char[0]) {
				case 'Z':
					// ready for query
					loop = false;
					break;
				case 'S':
					// parameter status
					_status[buffer.read0String().idup] = buffer.read0String().idup;
					break;
				case 'K':
					// backend key data
					_serverProcessId = buffer.read!(Endian.bigEndian, uint)();
					_serverSecretKey = buffer.read!(Endian.bigEndian, uint)();
					//loop = false;
					break;
				default:
					throw new DatabaseConnectionException("Wrong packet sequence");
			}
		} while(loop);
		// prepare a statement for table description
		prepareQuery(infoStatement, "select column_name, data_type, is_nullable, character_maximum_length, column_default from INFORMATION_SCHEMA.COLUMNS where table_name=$1;", Param.VARCHAR);
	}
	
	protected override void closeImpl() {
		_stream.socket.close();
	}

	private Buffer receive() {
		if(_error) {
			// clear packets received after an exception was thrown and not handled
			_error = false;
			string[] ids;
			do {
				receive();
				ids ~= _stream.id!char;
			} while(_stream.id!char[0] != 'Z');
			warning("An exception was thrown and ", ids.length, " packet(s) (", ids.join(", "), ") has been skipped");
		}
		Buffer buffer = _stream.receive();
		switch(_stream.id!char[0]) {
			case 'E':
				_error = true;
				PostgresqlDatabaseException[] exceptions;
				char errorCode;
				while((errorCode = buffer.read!char()) != '\0') {
					exceptions ~= new PostgresqlDatabaseException(errorCode, buffer.read0String());
				}
				throw new PostgresqlDatabaseExceptions(exceptions);
			case 'N':
				string[] notices;
				char noticeCode;
				while((noticeCode = buffer.read!char()) != '\0') {
					notices ~= buffer.read0String();
				}
				enforce!DatabaseConnectionException(notices.length >= 3, "Received malformed notice with " ~ notices.length.to!string ~ " fields");
				info("PostgreSQL (", notices[0], "): ", notices[3]);
				return receive();
			default:
				return buffer;
		}
	}

	private void sendFlush() {
		_stream.id = "H";
		Buffer buffer = new Buffer(5);
		_stream.send(buffer);
	}

	// QUERYING

	public override void query(string query) {
		trace("Running query `" ~ query ~ "`");
		_stream.id = "Q";
		Buffer buffer = new Buffer(query.length + 6);
		buffer.write0String(query);
		_stream.send(buffer);
	}

	public void prepareQuery(string statement, string query, Param[] params...) {
		trace("Preparing statement `" ~ statement ~ "` using `" ~ query ~ "`");
		_stream.id = "P";
		Buffer buffer = new Buffer(statement.length + query.length + 9 + params.length * 4);
		buffer.write0String(statement);
		buffer.write0String(query);
		buffer.write!(Endian.bigEndian, ushort)(params.length.to!ushort);
		foreach(param ; params) buffer.write!(Endian.bigEndian, uint)(param);
		_stream.send(buffer);
		sendFlush();
		receive();
		enforcePacketSequence('1');
	}

	public void executeQuery(string statement, Prepared.Param[] params...) {
		trace("Executing prepared statement `" ~ statement ~ "` with parameters " ~ params.to!string);
		Buffer buffer = new Buffer(512);
		_stream.id = "B";
		immutable length = params.length.to!ushort;
		buffer.write0String("");
		buffer.write0String(statement);
		buffer.write!(Endian.bigEndian, ushort)(length);
		foreach(param ; params) {
			static if(is(typeof(param) : string)) buffer.write!(Endian.bigEndian, ushort)(false);
			else buffer.write!(Endian.bigEndian, ushort)(true);
		}
		buffer.write!(Endian.bigEndian, ushort)(length);
		void writeImpl(T)(T value) {
			auto str = value.to!string;
			buffer.write!(Endian.bigEndian, uint)(str.length.to!uint);
			buffer.write(str);
		}
		foreach(param ; params) {
			if(param is null) {
				buffer.write!(Endian.bigEndian, uint)(uint.max);
			} else {
				final switch(param.type) with(Type) {
					case BOOL:
						writeImpl(param.to!string[0]);
						break;
					case BYTE:
					case SHORT:
					case INT:
					case LONG:
					case FLOAT:
					case DOUBLE:
					case CHAR:
					case STRING:
					case CLOB:
					case DATE:
					case DATETIME:
					case TIME:
						writeImpl(param.to!string);
						break;
					case BINARY:
					case BLOB:
						writeImpl(cast(string)(cast(Prepared.ParamImpl!(ubyte[], Type.BINARY))param).value);
						break;
				}
			}
		}
		buffer.write!(Endian.bigEndian, ushort)(1);
		buffer.write!(Endian.bigEndian, ushort)(1);
		_stream.send(buffer);
		buffer.reset();
		_stream.id = "E";
		buffer.write0String("");
		buffer.write(0);
		_stream.send(buffer);
		buffer.reset();
		_stream.id = "S";
		_stream.send(buffer);
		receiveAndEnforcePacketSequence('2');
	}
	
	public override Result querySelect(string query) {
		Result result;
		this.query(query);
		Buffer buffer = receive();
		if(_stream.id!char[0] != 'C') {
			enforcePacketSequence('T');
			ColumnInfo[] columns;
			foreach(i ; 0..buffer.read!(Endian.bigEndian, ushort)()) {
				ColumnInfo column;
				column.column = buffer.read0String().idup;
				buffer.readData(6);
				column.type = buffer.read!(Endian.bigEndian, uint)();
				buffer.readData(8);
				columns ~= column;
				result.columns[column.column] = i;
			}
			while(true) {
				buffer = receive();
				if(_stream.id!char[0] == 'C') break;
				enforcePacketSequence('D');
				enforce!DatabaseConnectionException(buffer.read!(Endian.bigEndian, ushort)() == columns.length, "Length of the row doesn't match the column's");
				Result.Row[] rows;
				foreach(column ; columns) {
					rows ~= parseRow(column.type, buffer);
				}
				result.rows ~= rows;
			}
		}
		enforceReadyForQuery();
		return result;
	}

	private Result.Row parseRow(uint param, Buffer buffer) {
		switch(param) with(Param) {
			case BOOL: return readString!bool(buffer);
			case BYTEA: return readString!(ubyte[])(buffer);
			case INT8: return readString!long(buffer);
			case INT2: return readString!short(buffer);
			case INT4: return readString!int(buffer);
			case TEXT: return readString!string(buffer);
			case FLOAT4: return readString!float(buffer);
			case FLOAT8: return readString!double(buffer);
			case CHAR: return readString!char(buffer);
			case VARCHAR: return readString!string(buffer);
			case DATE: return readString!(std.datetime.Date)(buffer);
			case TIMESTAMP: return readString!(std.datetime.DateTime)(buffer);
			case TIME: return readString!(std.datetime.TimeOfDay)(buffer);
			default: throw new DatabaseConnectionException("Unknwon type with id " ~ param.to!string);
		}
	}
	
	private static struct ColumnInfo {
		
		string column;
		uint type;

	}

	// CREATE | ALTER

	protected override TableInfo[string] getTableInfo(string table) {
		executeQuery(infoStatement, Prepared.prepare(table));
		TableInfo[string] ret;
		while(true) {
			Buffer buffer = receive();
			if(_stream.id!char[0] == 'C') break;
			enforcePacketSequence('D');
			enforce!DatabaseConnectionException(buffer.read!(Endian.bigEndian, ushort)() == 5, "Wrong number of fields returned by the server");
			TableInfo field;
			field.name = buffer.read!string(buffer.read!(Endian.bigEndian, uint)()).idup;
			field.type = fromStringToType(buffer.read!string(buffer.read!(Endian.bigEndian, uint)()));
			field.nullable = buffer.read!string(buffer.read!(Endian.bigEndian, uint)()) == "YES";
			immutable length = buffer.read!(Endian.bigEndian, uint)();
			if(length != uint.max) field.length = buffer.read!(Endian.bigEndian, uint)();
			immutable defaultValue = buffer.read!(Endian.bigEndian, uint)();
			if(defaultValue != uint.max) field.defaultValue = buffer.read!string(defaultValue).idup;
			ret[field.name] = field;
		}
		enforceReadyForQuery();
		return ret;
	}

	private uint fromStringToType(string str) {
		switch(str) with(Type) {
			case "boolean": return BOOL;
			case "smallint": return SHORT;
			case "integer": return INT;
			case "bigint": return LONG;
			case "real": return FLOAT;
			case "character": return CHAR;
			case "double precision": return DOUBLE;
			case "character varying": return STRING;
			case "bytea": return BINARY | BLOB;
			case "text": return CLOB;
			case "date": return DATE;
			case "timestamp": case "timestamp without time zone": return DATETIME;
			case "time": case "time without time zone": return TIME;
			default: throw new DatabaseException("Unknown type '" ~ str ~ "'");
		}
	}

	protected override string generateField(InitInfo.Field field) {
		string[] ret = [field.name];
		ret ~= fromTypeToString(cast(Type)field.type, field.autoIncrement, field.length);
		if(field.length) ret[1] ~= "(" ~ field.length.to!string ~ ")";
		if(!field.nullable) ret ~= "not null";
		if(field.unique) ret ~= "unique";
		return ret.join(" ");
	}
	
	private string fromTypeToString(Type type, bool autoIncrement, ref size_t length) {
		final switch(type) with(Type) {
			case BOOL: return "boolean";
			case BYTE: throw new DatabaseException("Type byte is not supported");
			case SHORT: return autoIncrement ? "serial2" : "int2";
			case INT: return autoIncrement ? "serial4" : "int4";
			case LONG: return autoIncrement ? "serial8" : "int8";
			case FLOAT: return "float4";
			case DOUBLE: return "float8";
			case CHAR:
				length = 1;
				return "char";
			case STRING: return "varchar";
			case BINARY:
			case BLOB:
				length = 0; // bytea(x) not supported
				return "bytea";
			case CLOB: return "text";
			case DATE: return "date";
			case DATETIME: return "timestamp";
			case TIME: return "time";
		}
	}

	protected override void createTable(string table, string[] fields) {
		super.createTable(table, fields);
		receiveAndEnforcePacketSequence('C');
		enforceReadyForQuery();
	}

	protected override void alterTableColumn(string table, InitInfo.Field field, bool typeChanged, bool nullableChanged) {
		string q = "alter table " ~ table ~ " alter column " ~ field.name;
		if(typeChanged) {
			q ~= " type " ~ fromTypeToString(cast(Type)field.type, false, field.length);
			if(field.length) q ~= "(" ~ field.length.to!string ~ ")";
		}
		if(nullableChanged) {
			if(field.nullable) q ~= " drop not null";
			else q ~= " set not null";
		}
		query(q ~ ";");
		receiveAndEnforcePacketSequence('C');
		enforceReadyForQuery();
	}

	protected override void alterTableAddColumn(string table, InitInfo.Field field) {
		super.alterTableAddColumn(table, field);
		receiveAndEnforcePacketSequence('C');
		enforceReadyForQuery();
	}

	protected override void alterTableDropColumn(string table, string column) {
		super.alterTableDropColumn(table, column);
		receiveAndEnforcePacketSequence('C');
		enforceReadyForQuery();
	}

	// INSERT

	protected override Result insertImpl(InsertInfo insertInfo) {
		auto ret = super.insertImpl(insertInfo);
		receiveAndEnforcePacketSequence('C');
		enforceReadyForQuery();
		return ret;
	}

	protected override Result insertInto(string table, string[] names, string[] fields, string[] primaryKeys) {
		string q = "insert into " ~ table ~ " (" ~ names.join(",") ~ ") values (" ~ fields.join(",") ~ ")";
		if(primaryKeys.length) q ~= " returning " ~ primaryKeys.join(",");
		query(q);
		if(primaryKeys.length) {
			Result result;
			Buffer buffer = receive();
			enforce(buffer.read!(Endian.bigEndian, ushort)() == primaryKeys.length, "Wrong number of fields returned by the server");
			ColumnInfo[] info;
			foreach(i ; 0..primaryKeys.length) {
				ColumnInfo column;
				column.column = buffer.read0String().idup;
				buffer.readData(6); // ???
				column.type = buffer.read!(Endian.bigEndian, uint)();
				buffer.readData(8); // ???
				info ~= column;
				result.columns[column.column] = i;
			}
			buffer = receive();
			enforce(buffer.read!(Endian.bigEndian, ushort)() == primaryKeys.length, "Wrong number of fields returned by the server");
			Result.Row[] rows;
			foreach(column ; info) {
				rows ~= parseRow(column.type, buffer);
			}
			result.rows ~= rows;
			return result;
		} else {
			return Result.init;
		}
	}

	// UPDATE

	protected override void updateImpl(UpdateInfo updateInfo, Clause.Where where) {
		super.updateImpl(updateInfo, where);
		receiveAndEnforcePacketSequence('C');
		enforceReadyForQuery();
	}

	// DELETE

	protected override void deleteImpl(string table, Clause.Where where) {
		super.deleteImpl(table, where);
		receiveAndEnforcePacketSequence('C');
		enforceReadyForQuery();
	}

	// DROP

	public override void dropIfExists(string table) {
		super.dropIfExists(table);
		receiveAndEnforcePacketSequence('C');
		enforceReadyForQuery();
	}

	public override void drop(string table) {
		super.drop(table);
		receiveAndEnforcePacketSequence('C');
		enforceReadyForQuery();
	}

	// UTILS

	private void enforcePacketSequence(char expected) {
		immutable current = _stream.id!char[0];
		if(current != expected) throw new WrongPacketSequenceException(expected, current);
	}

	private void receiveAndEnforcePacketSequence(char expected) {
		receive();
		enforcePacketSequence(expected);
	}

	private void enforceReadyForQuery() {
		Buffer buffer = receive();
		enforcePacketSequence('Z');
		enforce!DatabaseConnectionException(buffer.data.length == 1 && "ITE".canFind(buffer.read!char()), "Server is not ready for query");
	}

	protected override string randomFunction() {
		return "random()";
	}

	protected override string escapeBinary(ubyte[] value) {
		return "'\\x" ~ toHexString(value) ~ "'";
	}

	private enum Param : uint {

		BOOL = 16,
		BYTEA = 17,
		INT8 = 20,
		INT2 = 21,
		INT4 = 23,
		TEXT = 25,
		FLOAT4 = 700,
		FLOAT8 = 701,
		CHAR = 1042,
		VARCHAR = 1043,
		DATE = 1082,
		TIME = 1083,
		TIMESTAMP = 1114,

	}
	
	private Result.Row readString(T)(Buffer buffer) {
		immutable length = buffer.read!(Endian.bigEndian, uint)();
		if(length == uint.max) return null;
		else {
			auto str = buffer.read!string(length).idup;
			static if(is(T == ubyte[])) {
				assert(str.length > 2 && str.length % 2 == 0);
				return Result.Row.from(fromHexString(str[2..$]));
			} else static if(is(T == std.datetime.Date)) {
				return Result.Row.from(std.datetime.Date.fromISOExtString(str));
			} else static if(is(T == std.datetime.DateTime)) {
				return Result.Row.from(std.datetime.DateTime.fromISOExtString(str.replace(" ", "T")));
			} else static if(is(T == std.datetime.TimeOfDay)) {
				return Result.Row.from(std.datetime.TimeOfDay.fromISOExtString(str));
			} else static if(is(T == bool)) {
				return Result.Row.from(str == "t");
			} else {
				return Result.Row.from(to!T(str));
			}
		}
	}

}

class WrongPacketSequenceException : DatabaseConnectionException {

	public this(char expected, char got, string file=__FILE__, size_t line=__LINE__) {
		super("Wrong packet sequence (expected '" ~ expected ~ "' but got '" ~ got ~ "')", file, line);
	}

}

alias PostgresqlDatabaseException = ErrorCodeDatabaseException!("PostgreSQL", char);

alias PostgresqlDatabaseExceptions = ErrorCodesDatabaseException!PostgresqlDatabaseException;
