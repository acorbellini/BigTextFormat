package edu.bigtextformat.block;

import edu.bigtextformat.data.BlockData;

public abstract class BlockFormat {

	public abstract BlockFormat getKeyFormat();

	public abstract byte[] getKey(BlockData data);

	public abstract int compare(byte[] d1, byte[] d2);

	public static BlockFormat getFormat(String type, byte[] bs)
			throws Exception {
		BlockFormats f = BlockFormats.valueOf(type);
		return f.fromByteArray(bs);
	}
}
