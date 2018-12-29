module shark.impl.mysql;

import std.algorithm : max, canFind;
import std.conv : to;
import std.exception : enforce;
import std.digest.sha : sha1Of, sha256Of;
import std.socket;
import std.string : join;
import std.system : Endian;

import shark.database : DatabaseConnectionException, ErrorCodeDatabaseException;
import shark.sql : SqlDatabase;
import shark.util : Stream, read0String, write0String;

import xbuffer : Buffer;

// debug
import std.stdio;

enum CharacterSet : ubyte {

	latin1 = 8,
	latin2 = 9,
	ascii = 11,
	utf8 = 33,
	utf16 = 54,
	utf16le = 56,
	utf32 = 60,
	binary = 63

}

private enum CapabilityFlags : uint {

	longPassword = 0x00000001,
	foundRows = 0x00000002,
	longFlag = 0x00000004,
	connectWithDb = 0x00000008,
	noSchema = 0x000000010,
	compress = 0x00000020,
	odbc = 0x00000040,
	localFiles = 0x00000080,
	ignoreSpace = 0x00000100,
	protocol41 = 0x00000200,
	interactive = 0x00000400,
	ssl = 0x00000800,
	ignoreSigpipe = 0x00001000,
	transactions = 0x00002000,
	reserved = 0x00004000,
	secureConnection = 0x00008000,
	multiStatements = 0x00010000,
	multiResults = 0x00020000,
	psMultiResults = 0x00040000,
	pluginAuth = 0x00080000,

}

private alias MysqlStream = Stream!(0, Endian.littleEndian, 3, false, Endian.littleEndian, ubyte);

class MysqlDatabase : SqlDatabase {

	private immutable ubyte characterSet;

	private MysqlStream _stream;
	private void[] _buffer;

	private string _serverVersion;

	public this(string host, ushort port=3306, ubyte characterSet=CharacterSet.utf8) {
		this.characterSet = characterSet;
		Socket socket = new TcpSocket();
		socket.blocking = true;
		socket.connect(getAddress(host, port)[0]);
		_stream = new MysqlStream(socket, 1024);
	}

	/**
	 * Gets MySQL server's version as indicated in the handshake
	 * process by the server.
	 */
	public @property string serverVersion() {
		return _serverVersion;
	}
	
	protected override void connectImpl(string db, string user, string password) {
		Buffer buffer = _stream.receive();
		enforce!DatabaseConnectionException(buffer.read!ubyte() == 0x0a, "Incompatible protocols");
		_serverVersion = buffer.read0String();
		buffer.readData(4); // connection id
		ubyte[] authPluginData = buffer.read!(ubyte[])(8);
		buffer.readData(1); // filler
		uint capabilities = buffer.read!(Endian.littleEndian, ushort)();
		buffer.read!ubyte(); // character set
		buffer.readData(2); // status flags
		capabilities |= (buffer.read!(Endian.littleEndian, ushort)() << 16);
		immutable authPluginDataLength = buffer.read!byte();
		buffer.readData(10); // reserved
		if(capabilities & CapabilityFlags.secureConnection) {
			authPluginData ~= buffer.read!(ubyte[])(max(13, authPluginDataLength - 8));
			authPluginData = authPluginData[0..$-1]; // remove final 0
		}
		string method;
		if(capabilities & CapabilityFlags.pluginAuth) {
			method = buffer.read0String();
			enforce!DatabaseConnectionException(["mysql_native_password", "caching_sha2_password"].canFind(method), "Unknown hashing method '" ~ method ~ "'");
		}
		enforce!DatabaseConnectionException(capabilities & CapabilityFlags.protocol41, "Server does not support protocol v4.1");
		buffer.reset();
		buffer.write!(Endian.littleEndian, uint)(CapabilityFlags.protocol41 | CapabilityFlags.connectWithDb | CapabilityFlags.secureConnection | CapabilityFlags.pluginAuth);
		buffer.write!(Endian.littleEndian, uint)(1);
		buffer.write(characterSet);
		buffer.writeData(new void[23]); // reserved
		buffer.write0String(user);
		if(password.length) {
			immutable hash = method == "mysql_native_password" ? hashPassword1(password, authPluginData) : hashPassword2(password, authPluginData);
			buffer.write(hash.length & ubyte.max);
			buffer.write(hash);
		} else {
			buffer.write(ubyte(0));
		}
		buffer.write0String(db);
		buffer.write0String(method);
		_stream.send(buffer);
	}
	
	private string hashPassword1(string password, const(ubyte)[] nonce) {
		auto password1 = sha1Of(password);
		auto res = sha1Of(sha1Of(password1), nonce).dup;
		foreach(i, ref r; res) {
			r = r ^ password1[i];
		}
		return cast(string)res;
	}
	
	private string hashPassword2(string password, const(ubyte)[] nonce) {
		auto password1 = sha256Of(password);
		auto res = sha256Of(sha256Of(password1), nonce).dup;
		foreach(i, ref r; res) {
			r = r ^ password1[i];
		}
		return cast(string)res;
	}
	
	private Buffer receive() {
		Buffer buffer = _stream.receive();
		if(buffer.peek!ubyte() == 0xff) {
			buffer.readData(1);
			immutable errorCode = buffer.read!(Endian.littleEndian, ushort)();
			buffer.readData(6);
			throw new MysqlDatabaseException(errorCode, cast(string)buffer.data);
		}
		return buffer;
	}

	public override Buffer query(string query) {
		debug writeln("Running MySQL query: ", query);
		Buffer buffer = new Buffer(query.length + 1);
		buffer.write(ubyte(3));
		buffer.write(query);
		_stream.resetSequence();
		_stream.send(buffer);
		return receive();
	}

	protected override TableInfo[string] getTableInfo(string table) {
		query("describe " ~ table ~ ";");
		return null;
	}

	protected override string generateField(InitInfo.Field field) {
		string[] ret = [field.name];
		ret ~= convertType(field.type) ~ (field.length ? "(" ~ field.length.to!string ~ ")" : "");
		if(field.autoIncrement) ret ~= "auto_increment";
		if(!field.nullable) ret ~= "not null";
		if(field.unique) ret ~= "unique";
		return ret.join(" ");
	}

	private string convertType(Type type) {
		return "???"; //TODO
	}
	
	protected override void alterTableColumn(string table, InitInfo.Field field, bool typeChanged, bool nullableChanged) {
		query("alter table " ~ table ~ " modify column " ~ generateField(field) ~ ";");
	}

}

alias MysqlDatabaseException = ErrorCodeDatabaseException!("MySQL", ushort);
