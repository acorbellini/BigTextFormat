package edu.bigtextformat.index;

import java.util.Iterator;

import edu.bigtextformat.Range;

public interface Index {

	void close() throws Exception;

	byte[] get(byte[] byteArray) throws Exception;

	long getBlockPosition(byte[] key) throws Exception;

	Iterator<Long> iterator(Range to);

	void put(byte[] k, byte[] v, boolean ifNotPresent) throws Exception;

	void splitRange(Range orig, Range range, Range range2, byte[] pos)
			throws Exception;

}
