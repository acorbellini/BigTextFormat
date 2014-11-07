package edu.bigtextformat.levels.levelfile;

import java.util.Iterator;

import edu.bigtextformat.levels.DataBlock;

public class LevelFileReader implements Iterator<DataBlock> {

	private LevelFile file;

	private long pos;

	private DataBlock curr;

	public LevelFileReader(LevelFile levelFile) throws Exception {
		this.file = levelFile;
		this.pos = levelFile.getBlockFile().getFirstBlockPos();
		try {
			this.curr = file.getDataBlock(pos);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public DataBlock next() {
		DataBlock ret = curr;
		try {
			if (curr != null)
				curr = file.getDataBlock(curr.getBlock().getNextBlockPos());
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ret;

	}

	@Override
	public boolean hasNext() {
		if (curr == null)
			return false;
		return true;
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub
		
	}

}
