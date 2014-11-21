package edu.bigtextformat.levels.compactor;

import edu.bigtextformat.levels.DataBlock;

public interface Writer {
	public void add(DataBlock dataBlock) throws Exception;

	public void persist() throws Exception;

	public void add(byte[] k, byte[] v) throws Exception;
}
