package edu.bigtextformat.record;

public interface DataType<T> {
	byte[] toByteArray() throws Exception;

	T fromByteArray(byte[] data) throws Exception;
}
