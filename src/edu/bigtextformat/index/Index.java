package edu.bigtextformat.index;

import java.util.Iterator;

import edu.bigtextformat.Range;

public interface Index {

	Iterator<Long> iterator(Range to);

	long getBlockPosition(byte[] key) throws Exception;

	void put(byte[] key, long value) throws Exception;

	void splitRange(Range orig, Range range, Range range2, long pos)
			throws Exception;

}
