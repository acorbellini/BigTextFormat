package edu.bigtextformat.levels;

import edu.bigtextformat.block.Block;
import edu.bigtextformat.block.BlockFile;
import edu.bigtextformat.block.BlockFormat;

public interface DataBlock {

	public abstract byte[] toByteArray() throws Exception;

	public abstract DataBlock fromByteArray(byte[] data) throws Exception;

	public abstract int size();

	public abstract byte[] lastKey();

	public abstract byte[] firstKey();

	public abstract void setBlock(Block b);

	public abstract Block getBlock();

	public abstract long getPos();

	public abstract int getLen();

	public abstract boolean contains(byte[] k, BlockFormat format);

	public abstract DataBlockIterator iterator();

	public abstract String print(BlockFormat format);

	public abstract int indexSize();

	public abstract Pair<byte[], byte[]> getFirstBetween(byte[] from,
			boolean inclFrom, byte[] to, boolean inclTo, BlockFormat format);

	public abstract BlockFile getBlockFile() throws Exception;

	public abstract byte[] get(byte[] k, BlockFormat format);

}