package edu.bigtextformat.levels.datablock;

import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.levels.levelfile.LevelFile;
import edu.bigtextformat.util.Pair;

public interface DataBlock {

	public abstract boolean contains(byte[] k);

	public abstract byte[] firstKey();

	public abstract DataBlock fromByteArray(byte[] data) throws Exception;

	public abstract byte[] get(byte[] k);

	public abstract Long getBlockPos();

	public abstract LevelFile getFile();

	public abstract Pair<byte[], byte[]> getFirstBetween(byte[] from,
			boolean inclFrom, byte[] to, boolean inclTo);

	public abstract byte[] getKey(int i);

	public abstract Long getLen();

	public abstract byte[] getValue(int i);

	public abstract int indexSize();

	public abstract DataBlockIterator iterator();

	public abstract byte[] lastKey();

	public abstract String print(BlockFormat format);

	public abstract long size();

	public abstract byte[] toByteArray() throws Exception;

}