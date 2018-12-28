module shark.util;

import std.array : Appender;
import std.conv : to;
import std.socket : Socket, SocketException, lastSocketError;
import std.system : Endian;

import xbuffer : Buffer;

// debug
import std.stdio;

class Stream(Endian endianness, size_t length, Endian sequenceEndianness=Endian.littleEndian, S=Object) {

	private enum usesSequence = !is(S == Object);

	protected Socket _socket;
	
	private void[] _recv;
	private Buffer _buffer;
	private union {
		version(LittleEndian) struct {
			void[length] _lengthData;
			void[size_t.sizeof-length] filler;
		}
		version(BigEndian) struct {
			void[size_t.sizeof-length] filler;
			void[length] _lengthData;
		}
		size_t _length;
	}
	
	static if(usesSequence) private S sequence;
	
	public this(Socket socket, size_t buffer) {
		_socket = socket;
		_recv = new void[buffer];
		_buffer = new Buffer(buffer);
	}
	
	static if(usesSequence) public void resetSequence() {
		sequence = 0;
	}
	
	public Buffer receive() {
		if(_length == 0) return readLength();
		else return readBody();
	}
	
	private void receiveImpl() {
		immutable recv = _socket.receive(_recv);
		if(recv == Socket.ERROR) throw new SocketException(lastSocketError);
		else if(recv == 0) throw new SocketException("Connection timed out");
		_buffer.writeData(_recv[0..recv]);
	}
	
	private Buffer readLength() {
		if(_buffer.canRead(length)) {
			_lengthData = _buffer.readData(length);
			return readSequence();
		} else {
			receiveImpl();
			return readLength();
		}
	}
	
	private Buffer readSequence() {
		static if(usesSequence) {
			if(_buffer.canRead(S.sizeof)) {
				sequence = _buffer.read!(sequenceEndianness, S)();
				sequence++;
				writeln(sequence);
				return readBody();
			} else {
				receiveImpl();
				return readSequence();
			}
		} else {
			return readBody();
		}
	}
	
	private Buffer readBody() {
		if(_buffer.data.length >= length) {
			void[] ret = _buffer.readData(_length);
			_length = 0;
			return new Buffer(ret);
		} else {
			receiveImpl();
			return readBody();
		}
	}
	
	public void send(Buffer buffer) {
		writeLength(buffer);
		buffer.write!(sequenceEndianness, S)(sequence++, length);
		_socket.send(buffer.data);
	}
	
	protected void writeLength(Buffer buffer) {
		immutable rlength = _length;
		_length = buffer.data.length;
		buffer.write(_lengthData, 0);
		_length = rlength;
	}

}

string read0String(Buffer buffer) {
	Appender!string ret;
	char c;
	while((c = buffer.read!char()) != '\0') {
		ret.put(c);
	}
	return ret.data;
}

void write0String(Buffer buffer, string str) {
	buffer.writeData(cast(void[])str);
	buffer.write(ubyte(0));
}

string toSnakeCase(string input) {
	Appender!string output;
	foreach(c ; input) {
		if(c >= 'A' && c <= 'Z') {
			output.put('_');
			output.put(cast(char)(c + 32));
		} else {
			output.put(c);
		}
	}
	return output.data;
}

unittest {

	assert("testTest".toSnakeCase() == "test_test");

}
