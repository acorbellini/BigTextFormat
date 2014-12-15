package edu.bigtextformat.levels.datablock;

public class DataBlockIterator {
	int i = 0;
	private DataBlockImpl db;
	byte[] k;
	byte[] val;

	public DataBlockIterator(DataBlockImpl dataBlock) {
		this.db = dataBlock;
		advance();

	}

	public void advance() {
		if (i >= db.indexSize()) {
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
		return k != null;
		// return i < db.indexSize();
	}
}
