module shark.impl.postgresql;

import std.conv : to;
import std.digest : toHexString, LetterCase;
import std.digest.md : md5Of;
import std.exception : enforce;
import std.experimental.logger : trace;
import std.socket;
import std.string : join;
import std.system : Endian;

import shark.database : DatabaseException, DatabaseConnectionException, ErrorCodeDatabaseException, ErrorCodesDatabaseException;
import shark.sql : SqlDatabase;
import shark.util : Stream, read0String, write0String, fromHexString;

import xbuffer : Buffer;

// debug
import std.stdio;

private alias PostgresqlStream = Stream!(1, Endian.bigEndian, 4, true);

class PostgresqlDatabase : SqlDatabase {

	private PostgresqlStream _stream;

	private string[string] _status;

	private uint _serverProcessId;
	private uint _serverSecretKey;

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
				// hashed password
				void[] salt = buffer.readData(4);
				hashedPassword = "md5" ~ toHexString!(LetterCase.lower)(md5Of(toHexString!(LetterCase.lower)(md5Of(password, user)), salt)).idup;
				break;
			default:
				throw new DatabaseConnectionException("Unknown authentication method requested by the server");
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
					_status[buffer.read0String()] = buffer.read0String();
					break;
				case 'K':
					// backend key data
					_serverProcessId = buffer.read!(Endian.bigEndian, uint)();
					_serverSecretKey = buffer.read!(Endian.bigEndian, uint)();
					loop = false;
					break;
				default:
					throw new DatabaseConnectionException("Wrong packet sequence");
			}
		} while(loop);
	}

	private Buffer receive() {
		Buffer buffer = _stream.receive();
		if(_stream.id!char[0] == 'E') {
			PostgresqlDatabaseException[] exceptions;
			char errorCode;
			while((errorCode = buffer.read!char()) != '\0') {
				exceptions ~= new PostgresqlDatabaseException(errorCode, buffer.read0String());
			}
			throw new PostgresqlDatabaseExceptions(exceptions);
		} else if(_stream.id!char[0] == 'Z') {
			// skip
			return receive();
		} else {
			return buffer;
		}
	}

	// QUERYING

	public override Buffer query(string query) {
		trace("Running query `" ~ query ~ "`");
		_stream.id = "Q";
		Buffer buffer = new Buffer(query.length + 6);
		buffer.write0String(query);
		_stream.send(buffer);
		return receive();
	}
	
	public override SelectResult querySelect(string query) {
		SelectResult result;
		Buffer buffer = this.query(query);
		enforcePacketSequence('T');
		ColumnInfo[] columns;
		foreach(i ; 0..buffer.read!(Endian.bigEndian, ushort)()) {
			ColumnInfo column;
			column.column = buffer.read0String();
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
			SelectResult.Row[] rows;
			foreach(column ; columns) {
				rows ~= {
					switch(column.type) {
						case 16: return readString!bool(buffer);
						case 17: return readString!(ubyte[])(buffer);
						case 20: return readString!long(buffer);
						case 21: return readString!short(buffer);
						case 23: return readString!int(buffer);
						case 25: return readString!string(buffer);
						case 700: return readString!float(buffer);
						case 701: return readString!double(buffer);
						case 1042: return readString!char(buffer);
						case 1043: return readString!string(buffer);
						default: throw new DatabaseConnectionException("Unknwon type with id " ~ column.type.to!string);
					}
				}();
			}
			result.rows ~= rows;
		}
		return result;
	}

	private static struct ColumnInfo {

		string column;
		uint type;

	}

	// CREATE | ALTER

	protected override TableInfo[string] getTableInfo(string table) {
		Buffer buffer = query("select column_name, data_type, is_nullable, character_maximum_length, column_default from INFORMATION_SCHEMA.COLUMNS where table_name=" ~ escapeString(table) ~ ";");
		if(_stream.id!char[0] == 'C') return null;
		enforcePacketSequence('T');
		enforce!DatabaseConnectionException(buffer.read!(Endian.bigEndian, ushort)() == 5, "Wrong number of fields returned by the server");
		string[] columns;
		foreach(i ; 0..5) {
			columns ~= buffer.read0String();
			buffer.readData(18);
		}
		TableInfo[string] ret;
		while(true) {
			buffer = receive();
			if(_stream.id!char[0] == 'C') break;
			enforcePacketSequence('D');
			enforce!DatabaseConnectionException(buffer.read!(Endian.bigEndian, ushort)() == 5, "Wrong number of fields returned by the server");
			TableInfo field;
			field.name = buffer.read!string(buffer.read!(Endian.bigEndian, uint)());
			field.type = fromStringToType(buffer.read!string(buffer.read!(Endian.bigEndian, uint)()));
			field.nullable = buffer.read!string(buffer.read!(Endian.bigEndian, uint)()) == "YES";
			immutable length = buffer.read!(Endian.bigEndian, uint)();
			if(length != uint.max) field.length = to!size_t(buffer.read!string(length));
			immutable defaultValue = buffer.read!(Endian.bigEndian, uint)();
			if(defaultValue != uint.max) field.defaultValue = buffer.read!string(defaultValue);
			ret[field.name] = field;
		}
		return ret;
	}

	private uint fromStringToType(string str) {
		switch(str) with(Type) {
			case "boolean": return BOOL;
			case "smaillint": return SHORT;
			case "integer": return INT;
			case "bigint": return LONG;
			case "real": return FLOAT;
			case "character": return CHAR;
			case "double precision": return DOUBLE;
			case "character varying": return STRING;
			case "bytea": return BINARY | BLOB;
			case "text": return CLOB;
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
		}
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
	}

	// UTILS

	private void enforcePacketSequence(char expected) {
		immutable current = _stream.id!char[0];
		if(current != expected) throw new WrongPacketSequenceException(expected, current);
	}

	protected override string escapeBinary(ubyte[] value) {
		return "'\\x" ~ toHexString(value) ~ "'";
		//return "decode('" ~ toHexString(value) ~ "', 'hex')";
	}
	
	private SelectResult.Row readString(T)(Buffer buffer) {
		immutable length = buffer.read!(Endian.bigEndian, uint)();
		if(length == uint.max) return null;
		else {
			static if(is(T == ubyte[])) {
				string hex = buffer.read!string(length);
				assert(hex.length > 4 && hex.length % 2 == 0);
				return SelectResult.Row.from(fromHexString(hex[2..$]));
			}
			else static if(is(T == bool)) return SelectResult.Row.from(buffer.read!string(length) == "t");
			else return SelectResult.Row.from(to!T(buffer.read!string(length)));
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
