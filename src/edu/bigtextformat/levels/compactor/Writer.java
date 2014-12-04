package edu.bigtextformat.levels.compactor;

import java.util.List;

import edu.bigtextformat.levels.DataBlock;
import edu.bigtextformat.levels.levelfile.LevelFile;

public interface Writer {
	public void addDataBlock(DataBlock dataBlock) throws Exception;

	public void persist() throws Exception;

	public void add(byte[] k, byte[] v) throws Exception;
}
