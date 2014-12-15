package edu.bigtextformat.levels.levelfile;

import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.levels.DataBlock;
import edu.bigtextformat.levels.DataBlockIterator;
import edu.bigtextformat.levels.Pair;

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

	public DataBlock getDB() {
		if (db == null)
			try {
				db = file.getDataBlock(pos, false);
			} catch (Exception e) {
				e.printStackTrace();
			}
		return db;
	}

	public byte[] getMaxKey() {
		return maxKey;
	}

	@Override
	public byte[] toByteArray() throws Exception {
		return getDB().toByteArray();
	}

	@Override
	public DataBlock fromByteArray(byte[] data) throws Exception {
		return null;
	}

	@Override
	public long size() {
		return len;
	}

	@Override
	public byte[] lastKey() {
		return getMaxKey();
	}

	@Override
	public byte[] firstKey() {
		return getDB().firstKey();
	}

	// @Override
	// public Block getBlock() {
	// return getDB().getBlock();
	// }

	@Override
	public boolean contains(byte[] k, BlockFormat format) {
		return getDB().contains(k, format);
	}

	@Override
	public DataBlockIterator iterator() {
		return getDB().iterator();
	}

	@Override
	public String print(BlockFormat format) {
		return getDB().print(format);
	}

	@Override
	public int indexSize() {
		return getDB().indexSize();
	}

	@Override
	public Pair<byte[], byte[]> getFirstBetween(byte[] from, boolean inclFrom,
			byte[] to, boolean inclTo, BlockFormat format) {
		return getDB().getFirstBetween(from, inclFrom, to, inclTo, format);
	}

	@Override
	public Long getLen() {
		return len;
	}

	@Override
	public LevelFile getFile() {
		return file;
	}

	@Override
	public byte[] get(byte[] k, BlockFormat format) {
		return getDB().get(k, format);
	}

	@Override
	public Long getBlockPos() {
		return pos;
	}

	@Override
	public byte[] getKey(int i) {
		return getDB().getKey(i);
	}

	@Override
	public byte[] getValue(int i) {
		return getDB().getValue(i);
	}
}
