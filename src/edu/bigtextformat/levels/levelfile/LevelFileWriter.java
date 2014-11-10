package edu.bigtextformat.levels.levelfile;

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
			f.put(curr.getDB());
			curr.clear();
		}
		curr.add(k, val);
	}

	public void close() throws Exception {
		f.put(curr.getDB());
	}

}
