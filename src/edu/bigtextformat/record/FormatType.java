package edu.bigtextformat.record;

public interface FormatType<T> {

	T get(byte[] k);

	int compare(byte[] d1, int offsetd1, byte[] d2, int offsetd2);

	int size(int offset, byte[] d);

	byte[] getData(int offset, byte[] d);

	byte[] toBytes(Object object);

}
