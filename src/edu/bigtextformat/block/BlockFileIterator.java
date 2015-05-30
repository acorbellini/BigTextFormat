package edu.bigtextformat.block;

import java.util.Iterator;

class BlockFileIterator implements Iterator<Block> {
	Block current = null;
	private BlockFile file;

	public BlockFileIterator(BlockFile file) throws Exception {
		this.file = file;
	}

	@Override
	public boolean hasNext() {
		try {
			// do {
			if (current == null)
				current = file.getFirstBlock();
			else
				current = file.getBlock(current.getNextBlockPos());
			// } while (current.isDeleted());
			return true;
		} catch (MissingBlockException e) {
			// No more blocks.. I guess
			return false;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public Block next() {
		return current;
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub

	}
}