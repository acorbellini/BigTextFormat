package edu.bigtextformat.levels.levelfile;

import edu.bigtextformat.levels.DataBlock;
import edu.bigtextformat.levels.DataBlockWriter;
import edu.bigtextformat.levels.LevelOptions;

public class LevelFileWriter {
	DataBlockWriter curr;
	private LevelFile f;
	private LevelOptions opts;

	public LevelFileWriter(LevelFile levelFile, LevelOptions opts) {
		this.f = levelFile;
		this.opts = opts;
		this.curr = new DataBlockWriter();
	}

	public void add(byte[] k, byte[] val) throws Exception {
		if (curr.size() >= opts.maxBlockSize) {
			flushCurrentBlock();
		}
		curr.add(k, val);
	}

	private void flushCurrentBlock() throws Exception {
		f.put(curr.getDB());
		curr.clear();
	}

	public void close() throws Exception {
		if (curr.size() > 0)
			f.put(curr.getDB());
	}

	public void add(DataBlock dataBlock) throws Exception {
		if (curr.size() > 0)
			flushCurrentBlock();
		f.put(dataBlock);
	}

}
