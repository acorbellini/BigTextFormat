package edu.bigtextformat.levels;

public class DataBlockIterator {
	int i = 0;
	private DataBlock db;
	byte[] k;
	byte[] val;

	public DataBlockIterator(DataBlock dataBlock) {
		this.db = dataBlock;
	}

	public void advance() {
		if (!hasNext()) {
			k = null;
			val = null;
			return;
		}
		k = db.getKey(i);
		val = db.getValue(i);
		i++;
	}

	public byte[] getKey() {
		return k;
	}

	public byte[] getVal() {
		return val;
	}

	public boolean hasNext() {
		return i < db.indexSize();
	}
}
