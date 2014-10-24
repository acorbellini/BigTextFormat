package edu.bigtextformat.index;

import java.util.Iterator;

import edu.bigtextformat.Range;
import edu.bigtextformat.record.Record;

public interface Index {

	Iterator<Long> iterator(Range to);

	long getBlockPosition(byte[] key) throws Exception;

	void put(byte[] record, byte[] bs) throws Exception;

	void splitRange(Range orig, Range range, Range range2, byte[] pos)
			throws Exception;

	byte[] get(byte[] byteArray) throws Exception;

}
