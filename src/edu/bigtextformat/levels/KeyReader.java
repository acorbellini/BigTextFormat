package edu.bigtextformat.levels;

import java.util.AbstractList;

public class KeyReader extends AbstractList<byte[]> {
	DataBlock db;

	public KeyReader(DataBlock db) {
		this.db = db;
	}

	@Override
	public byte[] get(int index) {
		return db.get(index);
	}

	@Override
	public int size() {
		return db.indexSize();
	}

}
