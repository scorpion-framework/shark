module shark.util;

import std.array : Appender;
import std.conv : to;
import std.socket : Socket, SocketException, lastSocketError;
import std.system : Endian, endian;

import xbuffer : Buffer;

// debug
import std.stdio;

class Stream(size_t idLength, Endian endianness, size_t length, bool lengthIncludesItself, Endian sequenceEndianness=Endian.littleEndian, S=Object) {

	private enum usesSequence = !is(S == Object);

	protected Socket _socket;
	
	private void[] _recv;
	private Buffer _buffer;
	private union {
		version(LittleEndian) struct {
			ubyte[length] _lengthData;
			ubyte[size_t.sizeof-length] filler;
		}
		version(BigEndian) struct {
			ubyte[size_t.sizeof-length] filler;
			ubyte[length] _lengthData;
		}
		size_t _length;
	}

	private Buffer _returnBuffer;

	static if(idLength) private void[idLength] _id;
	
	static if(usesSequence) private S sequence;
	
	public this(Socket socket, size_t buffer) {
		_socket = socket;
		_recv = new void[buffer];
		_buffer = new Buffer(buffer);
		_returnBuffer = new Buffer(buffer);
	}

	public @property Socket socket() {
		return _socket;
	}

	static if(idLength) public @property T[idLength] id(T=void)() {
		return cast(T[idLength])_id;
	}

	static if(idLength) public @property T[idLength] id(T)(T[idLength] id) {
		_id = cast(void[])id;
		return id;
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
		static if(idLength) immutable requiredLength = idLength + length;
		else immutable requiredLength = length;
		if(_buffer.canRead(requiredLength)) {
			static if(idLength) _id = _buffer.readData(idLength);
			_lengthData = _buffer.read!(ubyte[])(length);
			static if(endianness != endian) reverse(_lengthData);
			static if(lengthIncludesItself) _length -= length;
			return readSequence();
		} else {
			receiveImpl();
			return readLength();
		}
	}
	
	private Buffer readSequence() {
		static if(usesSequence) {
			if(_buffer.canRead(S.sizeof)) {
				static if(usesSequence) {
					_buffer.read!(sequenceEndianness, S)();
					sequence++;
				}
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
		if(_buffer.data.length >= _length) {
			_returnBuffer.data = _buffer.readData(_length);
			_length = 0;
			return _returnBuffer;
		} else {
			receiveImpl();
			return readBody();
		}
	}
	
	public void send(Buffer buffer) {
		writeLength(buffer);
		static if(usesSequence) buffer.write!(sequenceEndianness, S)(sequence++, length);
		static if(idLength) buffer.write(_id, 0);
		if(_socket.send(buffer.data) == Socket.ERROR) throw new SocketException(lastSocketError);
	}
	
	protected void writeLength(Buffer buffer) {
		immutable rlength = _length;
		_length = buffer.data.length;
		static if(lengthIncludesItself) _length += length;
		static if(endianness != endian) reverse(_lengthData);
		buffer.write(_lengthData, 0);
		_length = rlength;
	}

}

private void reverse(T)(ref T array) {
	T ret;
	foreach(i ; 0..array.length) {
		ret[$-1-i] = array[i];
	}
	array = ret;
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

ubyte[] fromHexString(string hex) {
	ubyte[] ret = new ubyte[hex.length / 2];
	foreach(i ; 0..ret.length) {
		ret[i] = to!ubyte(hex[i*2..i*2+2], 16);
	}
	return ret;
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
