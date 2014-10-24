package edu.bigtextformat.record;

public interface FormatType<T> {

	T get(byte[] k);

	int compare(byte[] k1, byte[] k2);

	int size(int offset, byte[] d);

	byte[] getData(int offset, byte[] d);

	byte[] toBytes(Object object);

}
