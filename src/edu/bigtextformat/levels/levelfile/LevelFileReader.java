package edu.bigtextformat.levels.levelfile;

import java.util.Iterator;

import edu.bigtextformat.levels.Index;

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
			if (indexPos == index.size() - 1) {
				len = (int) (file.getFile().getLastBlockPosition() - pos);
			} else
				len = (int) (index.getBlocks().get(indexPos + 1) - pos);
			if (len < 0)
				throw new Exception("Length is < 0 on index at file (Index unordered or corrupted)"
						+ file.getFile().getRawFile().getFile());

			this.curr = new DataBlockReference(file, currMaxKey, pos, len);
			indexPos++;
		} else
			this.curr = null;
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
	public boolean hasNext() {
		return curr != null;
	}

	@Override
	public void remove() {
	}

}
