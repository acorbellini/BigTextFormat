package edu.bigtextformat.levels.levelfile;

import java.util.List;

import edu.bigtextformat.levels.DataBlock;
import edu.bigtextformat.levels.LevelOptions;

public class LevelFileWriter {
	DataBlock curr;
	private LevelFile f;
	private LevelOptions opts;

	public LevelFileWriter(LevelFile levelFile, LevelOptions opts) {
		this.f = levelFile;
		this.opts = opts;
		this.curr = new DataBlock();
	}

	public void add(byte[] k, byte[] val) throws Exception {
		if (curr.size() >= opts.maxBlockSize) {
			f.put(curr);
			curr.clear();
		}
		curr.add(k, val);
	}

	public void close() throws Exception {
		f.put(curr);
	}

	public void add(DataBlock db) throws Exception {
		List<byte[]> keys = db.getKeys();
		List<byte[]> values = db.getValues();
		for (int i = 0; i < keys.size(); i++) {
			add(keys.get(i), values.get(i));
		}
	}

	public void add(LevelFile levelFile) {
	}

}
