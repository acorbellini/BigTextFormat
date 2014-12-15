package edu.bigtextformat.levels.datablock;

import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.levels.levelfile.LevelFile;
import edu.bigtextformat.util.Pair;

public interface DataBlock {

	public abstract byte[] toByteArray() throws Exception;

	public abstract DataBlock fromByteArray(byte[] data) throws Exception;

	public abstract long size();

	public abstract byte[] lastKey();

	public abstract byte[] firstKey();

	public abstract Long getLen();

	public abstract boolean contains(byte[] k, BlockFormat format);

	public abstract DataBlockIterator iterator();

	public abstract String print(BlockFormat format);

	public abstract int indexSize();

	public abstract LevelFile getFile();

	public abstract Pair<byte[], byte[]> getFirstBetween(byte[] from,
			boolean inclFrom, byte[] to, boolean inclTo, BlockFormat format);

	public abstract byte[] get(byte[] k, BlockFormat format);

	public abstract Long getBlockPos();

	public abstract byte[] getKey(int i);

	public abstract byte[] getValue(int i);

}