package edu.bigtextformat.record;

public interface DataType<T> {
	T fromByteArray(byte[] data) throws Exception;

	byte[] toByteArray() throws Exception;
}
