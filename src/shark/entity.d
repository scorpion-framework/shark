module shark.entity;

interface Entity {

	public @property string tableName();

}

struct Nullable(T) {

	private bool _isNull = true;
	private T _value;
	
	public @property bool isNull() {
		return _isNull;
	}
	
	public @property T value() {
		return _value;
	}
	
	public @property T value(T value) {
		_isNull = false;
		return (_value = value);
	}
	
	alias value this;

}

alias Bool = Nullable!bool;

alias Byte = Nullable!byte;

alias Short = Nullable!short;

alias Integer = Nullable!int;

alias Long = Nullable!long;

alias String = Nullable!string;

alias Binary = Nullable!(ubyte[]);

struct Name { string name; }

enum Id;

enum AutoIncrement;

enum NotNull;

enum Unique;

struct Length { size_t length; }

enum Lob;
