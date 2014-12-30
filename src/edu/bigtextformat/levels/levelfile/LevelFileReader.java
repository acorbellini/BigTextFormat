package edu.bigtextformat.levels.levelfile;

import java.util.Iterator;

import edu.bigtextformat.levels.datablock.DataBlockReference;
import edu.bigtextformat.levels.index.Index;

public class LevelFileReader implements Iterator<DataBlockReference> {

	private LevelFile file;

	private long pos;

	private DataBlockReference curr;

	int indexPos = 0;

	private Index index;

	public LevelFileReader(LevelFile levelFile) throws Exception {
		this.file = levelFile;
		// this.pos = levelFile.getBlockFile().getFirstBlockPos();
		index = levelFile.getIndex();
		advance();
	}

	private void advance() throws Exception {
		if (indexPos < index.size()) {

			byte[] currMaxKey = index.getKeys().get(indexPos);
			this.pos = index.getBlocks().get(indexPos);

			int len = 0;
			long nextPos = index.getNextBlockTo(pos);
			if (nextPos < 0)
				nextPos = file.getLastBlockPosition();
			len = (int) (nextPos - pos);
			if (len < 0)
				throw new Exception(
						"Length is < 0 on index at file (Index unordered or corrupted)"
								+ file.getPath());

			this.curr = new DataBlockReference(file, currMaxKey, pos, len);
			indexPos++;
		} else
			this.curr = null;
	}

	public LevelFile getFile() {
		return file;
	}

	@Override
	public boolean hasNext() {
		return curr != null;
	}

	@Override
	public DataBlockReference next() {
		DataBlockReference ret = curr;
		try {
			if (hasNext())
				advance();
			// curr = file.getDataBlock(curr.getBlock().getNextBlockPos());
		} catch (Exception e) {
			e.printStackTrace();
			curr = null;
		}
		return ret;

	}

	@Override
	public void remove() {
	}

}
