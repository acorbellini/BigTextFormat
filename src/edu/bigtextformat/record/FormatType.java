package edu.bigtextformat.record;

public interface FormatType<T> {

	int compare(byte[] d1, int offsetd1, byte[] d2, int offsetd2);

	T get(byte[] k);

	byte[] getData(int offset, byte[] d);

	FormatTypes getType();

	int size(int offset, byte[] d);

	byte[] toBytes(Object object);

}
