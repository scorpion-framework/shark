module shark.entity;

interface Entity {

	public @property string tableName();

}

struct Nullable(T, ubyte id=0) if(!is(T : Object)) {

	private bool _isNull = true;
	private T _value;
	
	public @property bool isNull() {
		return _isNull;
	}

	public void nullify() {
		_isNull = true;
	}
	
	public @property T value() {
		return _value;
	}
	
	public @property T value(T value) {
		_isNull = false;
		return (_value = value);
	}

	public @property T value(Object object) {
		assert(object is null);
		nullify();
		return _value;
	}

	string toString() {
		import std.conv : to;
		return isNull ? "null" : value.to!string;
	}
	
	alias value this;

}

alias Bool = Nullable!bool;

alias Byte = Nullable!byte;

alias Short = Nullable!short;

alias Integer = Nullable!int;

alias Long = Nullable!long;

alias Float = Nullable!float;

alias Double = Nullable!double;

alias Char = Nullable!char;

alias String = Nullable!(string, 0);

alias Binary = Nullable!(ubyte[], 0);

alias Clob = Nullable!(string, 1);

alias Blob = Nullable!(ubyte[], 1);

struct Name { string name; }

enum PrimaryKey;

alias Id = PrimaryKey;

enum AutoIncrement;

enum NotNull;

enum Unique;

struct Length { size_t length; }
