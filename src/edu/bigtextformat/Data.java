package edu.bigtextformat;

import java.util.Iterator;

import edu.bigtextformat.block.Block;
import edu.bigtextformat.data.BlockData;

public interface Data {
	public void addData(BlockData data) throws Exception;

	public Iterator<Block> iterator(byte[] from, byte[] to) throws Exception;
}
