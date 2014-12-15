package edu.bigtextformat.block;

import edu.bigtextformat.data.BlockData;
import edu.bigtextformat.record.DataType;

public abstract class BlockFormat implements DataType<BlockFormat> {

	public static BlockFormat getFormat(BlockFormats f, byte[] bs)
			throws Exception {
		return f.fromByteArray(bs);
	}

	public abstract int compare(byte[] d1, byte[] d2);

	public abstract byte[] getKey(BlockData data);

	public abstract BlockFormat getKeyFormat();

	public abstract BlockFormats getType();

	public abstract String print(byte[] bs);
}
