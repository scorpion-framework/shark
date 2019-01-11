module shark.clause;

import std.conv : to;

/**
 * Clauses for select, update and delete.
 */
public struct Clause {
	
	@disable this();
	
	/**
	 * Where clause.
	 */
	static struct Where {
		
		GenericStatement statement;
		
		static interface GenericStatement {}
		
		static class Statement : GenericStatement {
			
			string field;
			
			Operator operator;
			
			string value;
			
			bool needsEscaping;
			
			this(T)(string field, Operator operator, T value, bool variable=false) {
				this.field = field;
				this.operator = operator;
				this.value = value.to!string;
				static if(is(T : string)) needsEscaping = !variable;
			}
			
			ComplexStatement opBinary(string op : "&")(GenericStatement statement) {
				return new ComplexStatement(this, Glue.and, statement);
			}
			
			ComplexStatement opBinary(string op : "|")(GenericStatement statement) {
				return new ComplexStatement(this, Glue.or, statement);
			}
			
		}
		
		static class ComplexStatement : GenericStatement {
			
			GenericStatement leftStatement;
			
			Glue glue;
			
			GenericStatement rightStatement;
			
			this(GenericStatement leftStatement, Glue glue, GenericStatement rightStatement) {
				this.leftStatement = leftStatement;
				this.glue = glue;
				this.rightStatement = rightStatement;
			}
			
		}
		
		enum Operator {
			
			isNull,
			equals,
			notEquals,
			greaterThan,
			greaterThanOrEquals,
			lessThan,
			lessThanOrEquals,
			
		}
		
		enum Glue {
			
			and,
			or
			
		}
		
	}
	
	/**
	 * Order clause.
	 * Example:
	 * ---
	 * Order(Order.Field("a", Order.Field.desc), Order.Field("b", Order.Field.asc));
	 * ---
	 */
	static struct Order {
		
		/**
		 * Random order.
		 */
		enum random = { Order order; order.rand=true; return order; }();
		
		bool rand = false;
		
		Field[] fields;
		
		this(Field[] fields...) {
			this.fields = fields;
		}
		
		this(string[] fields...) {
			foreach(field ; fields) this.fields ~= Field(field);
		}
		
		static struct Field {
			
			enum asc = true;
			
			enum desc = false;
			
			string name;
			
			bool _asc = true;
			
		}
		
	}
	
	/**
	 * Indicates the limit of rows to be returned. It can be single
	 * using the 1-field constructor or complex (lower and upper limit)
	 * using the 2-field constrcutor.
	 * Example:
	 * ---
	 * Limit(1);
	 * Limit(10, 20);
	 * ---
	 */
	static struct Limit {
		
		size_t lower, upper;
		
		this(size_t lower, size_t upper) {
			assert(lower < upper);
			this.lower = lower;
			this.upper = upper;
		}
		
		this(size_t limit) {
			this(0, limit);
		}
		
	}
	
}

struct Var {

	string var;

	private auto impl(T)(T value, Clause.Where.Operator operator) {
		static if(is(T : Var!V, V)) return new Clause.Where.Statement(var, operator, value, true);
		else return new Clause.Where.Statement(var, operator, value, false);
	}

	auto isNull() {
		return impl("", Clause.Where.Operator.isNull);
	}
	
	auto equals(T)(T value) {
		return impl(value, Clause.Where.Operator.equals);
	}
	
	auto notEquals(T)(T value) {
		return impl(value, Clause.Where.Operator.notEquals);
	}
	
	auto greaterThan(T)(T value) {
		return impl(value, Clause.Where.Operator.greatherThan);
	}
	
	auto greaterThanOrEquals(T)(T value) {
		return impl(value, Clause.Where.Operator.greatherThanOrEquals);
	}
	
	auto lessThan(T)(T value) {
		return impl(value, Clause.Where.Operator.lessThan);
	}
	
	auto lessThanOrEquals(T)(T value) {
		return impl(value, Clause.Where.Operator.lessThanOrEquals);
	}

}

Var var(string variable) {
	return Var(variable);
}
