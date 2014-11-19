package edu.bigtextformat.levels;

import java.util.AbstractList;

public class KeyReader extends AbstractList<byte[]> {
	DataBlockImpl db;

	public KeyReader(DataBlockImpl db) {
		this.db = db;
	}

	@Override
	public byte[] get(int index) {
		return db.getKey(index);
	}

	@Override
	public int size() {
		return db.indexSize();
	}

}
