module shark.mysql.database;

import std.algorithm : max;
import std.exception : enforce;
import std.socket;

import shark.database;
import shark.util : Stream, read0String, write0String;

import xbuffer;

// debug
import std.stdio;

private enum bufferSize = 512;

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

enum CapabilityFlags : uint {

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

private alias MysqlStream = Stream!(Endian.littleEndian, 3, Endian.littleEndian, ubyte);

class MysqlDatabase : Database {

	private immutable ubyte characterSet;

	private MysqlStream _stream;
	private void[] _buffer;

	public this(string host, ushort port, ubyte characterSet=CharacterSet.utf8) {
		this.characterSet = characterSet;
		Socket socket = new TcpSocket();
		socket.blocking = true;
		socket.connect(getAddress(host, port)[0]);
		_stream = new MysqlStream(socket, bufferSize);
	}
	
	protected override void connectImpl(string db, string user, string password) {
		Buffer buffer = _stream.receive();
		enforce!DatabaseConnectionException(buffer.read!ubyte() == 0x0a, "Incompatible protocol");
		buffer.read0String();
		buffer.readData(4); // connection id
		buffer.readData(8); // auth-plugin-data-part-1
		buffer.readData(1); // filler
		uint capabilities = buffer.read!(Endian.littleEndian, ushort)();
		if(buffer.data.length) {
			buffer.read!ubyte(); // character set
			buffer.readData(2); // status flags
			capabilities |= (buffer.read!(Endian.littleEndian, ushort)() << 16);
			immutable authPluginData = buffer.read!byte();
			buffer.readData(10); // reserved
			if(capabilities & CapabilityFlags.secureConnection) {
				buffer.readData(max(13, authPluginData - 8)); // auth-plugin-data-part-2
			}
			if(capabilities & CapabilityFlags.pluginAuth) {
				enforce!DatabaseConnectionException(buffer.read0String() == "caching_sha2_password", "Unknown hashing method");
			}
		}
		enforce!DatabaseConnectionException(capabilities & CapabilityFlags.protocol41, "Server did not send PROTOCOL_41 capability flag");
		buffer.reset();
		buffer.write!(Endian.littleEndian, uint)(CapabilityFlags.protocol41 | CapabilityFlags.connectWithDb);
		buffer.write!(Endian.littleEndian, uint)(int.max);
		buffer.write(characterSet);
		buffer.writeData(new void[23]); // reserved
		buffer.write0String(user);
		buffer.write0String(db);
		_stream.send(buffer);
		buffer = receive();
		writeln(buffer.data);
		writeln(cast(string)buffer.data);
	}
	
	private Buffer receive() {
		Buffer buffer = _stream.receive();
		if(buffer.read!ubyte == 0xff) {
			immutable errorCode = buffer.read!(Endian.littleEndian, ushort)();
			buffer.readData(6);
			throw new MysqlDatabaseException(errorCode, cast(string)buffer.data);
		}
		return buffer;
	}

}

class MysqlDatabaseException : ErrorCodeDatabaseException!("mysql", ushort) {

	public this(ushort errorCode, string msg, string file=__FILE__, size_t line=__LINE__) {
		super(errorCode, msg, file, line);
	}

}

unittest {

	Database database = new MysqlDatabase("localhost", 3306);
	database.connect("test", "root", "root");

}
