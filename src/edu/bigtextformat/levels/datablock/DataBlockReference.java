package edu.bigtextformat.levels.datablock;

import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.levels.levelfile.LevelFile;
import edu.bigtextformat.util.Pair;

public class DataBlockReference implements DataBlock {

	DataBlock db;

	LevelFile file;
	byte[] maxKey;
	long pos;

	private Long len;

	public DataBlockReference(LevelFile file, byte[] maxKey, long pos, long len) {
		super();
		this.file = file;
		this.maxKey = maxKey;
		this.pos = pos;
		this.len = len;
	}

	@Override
	public boolean contains(byte[] k) {
		return getDB().contains(k);
	}

	@Override
	public byte[] firstKey() {
		return getDB().firstKey();
	}

	@Override
	public DataBlock fromByteArray(byte[] data) throws Exception {
		return null;
	}

	@Override
	public byte[] get(byte[] k) {
		return getDB().get(k);
	}

	@Override
	public Long getBlockPos() {
		return pos;
	}

	public DataBlock getDB() {
		if (db == null)
			try {
				db = file.getDataBlock(pos, false);
			} catch (Exception e) {
				e.printStackTrace();
			}
		return db;
	}

	@Override
	public LevelFile getFile() {
		return file;
	}

	// @Override
	// public Block getBlock() {
	// return getDB().getBlock();
	// }

	@Override
	public Pair<byte[], byte[]> getFirstBetween(byte[] from, boolean inclFrom,
			byte[] to, boolean inclTo) {
		return getDB().getFirstBetween(from, inclFrom, to, inclTo);
	}

	@Override
	public byte[] getKey(int i) {
		return getDB().getKey(i);
	}

	@Override
	public Long getLen() {
		return len;
	}

	public byte[] getMaxKey() {
		return maxKey;
	}

	@Override
	public byte[] getValue(int i) {
		return getDB().getValue(i);
	}

	@Override
	public int indexSize() {
		return getDB().indexSize();
	}

	@Override
	public DataBlockIterator iterator() {
		return getDB().iterator();
	}

	@Override
	public byte[] lastKey() {
		return getMaxKey();
	}

	@Override
	public String print(BlockFormat format) {
		return getDB().print(format);
	}

	@Override
	public long size() {
		return len;
	}

	@Override
	public byte[] toByteArray() throws Exception {
		return getDB().toByteArray();
	}
}
