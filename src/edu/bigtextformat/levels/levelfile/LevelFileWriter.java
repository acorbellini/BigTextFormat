package edu.bigtextformat.levels.levelfile;

import java.util.ArrayList;
import java.util.List;

import edu.bigtextformat.levels.LevelOptions;
import edu.bigtextformat.levels.datablock.DataBlock;
import edu.bigtextformat.levels.datablock.DataBlockImpl;
import edu.bigtextformat.levels.datablock.DataBlockWriter;

public class LevelFileWriter {
	DataBlockWriter curr;

	List<DataBlock> created = new ArrayList<>();

	private LevelFile f;
	private LevelOptions opts;
	long size = 0;

	public LevelFileWriter(LevelFile levelFile, LevelOptions opts) {
		this.f = levelFile;
		this.opts = opts;
		this.curr = new DataBlockWriter();
	}

	public void add(byte[] k, byte[] val) throws Exception {
		if (curr.size() >= opts.maxBlockSize) {
			// System.out.println("Block surpased max size, flushing to disk.");
			flushCurrentBlock();
		}
		curr.add(k, val);
	}

	private void add(DataBlock db) {
		created.add(db);
		size += db.size();
	}

	public void addDatablock(DataBlock dataBlock) throws Exception {
		flushCurrentBlock();
		// f.put(dataBlock);
		add(dataBlock);
	}

	public void close() throws Exception {
		flushCurrentBlock();
		for (DataBlock dataBlock : created) {
			f.put(dataBlock);
		}
	}

	private void flushCurrentBlock() throws Exception {
		if (curr.size() > 0) {
			// f.put(curr.getDB());
			DataBlockImpl db = curr.getDB();
			add(db);
			curr.clear();
		}
	}

	public long size() {
		return size;
	}

}
