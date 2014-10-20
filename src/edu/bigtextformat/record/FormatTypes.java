package edu.bigtextformat.record;

public enum FormatTypes {
	INTEGER(new IntegerType()), LONG(new LongType()), STRING(new StringType()), DATE(
			new DateType()), BYTEARRAY(new ByteArrayType());
	FormatType<?> type;

	private FormatTypes(FormatType<?> type) {
		this.type = type;
	}

	public FormatType<?> getType() {
		return type;
	}
}
