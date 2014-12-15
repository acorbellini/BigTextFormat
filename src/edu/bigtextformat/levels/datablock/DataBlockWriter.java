package edu.bigtextformat.levels.datablock;

import edu.bigtextformat.util.ByteArrayList;

public class DataBlockWriter {
	ByteArrayList keys = new ByteArrayList();
	ByteArrayList values = new ByteArrayList();
	private int size = 0;

	public synchronized void add(byte[] k, byte[] val) {
		keys.add(k);
		values.add(val);
		size += k.length + val.length + 4 + 4;
	}

	public void clear() {
		keys.clear();
		values.clear();
		size = 0;
	}

	public DataBlockImpl getDB() {
		return new DataBlockImpl(keys.getVals().toArray(), keys.getIndexes()
				.toArray(), values.getVals().toArray(), values.getIndexes()
				.toArray());
	}

	public int size() {
		return size; //for each list size (2 32-bit integers)
	}

}
