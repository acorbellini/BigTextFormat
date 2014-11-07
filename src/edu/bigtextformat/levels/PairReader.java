package edu.bigtextformat.levels;

import java.util.Iterator;

import edu.bigtextformat.levels.levelfile.LevelFile;
import edu.bigtextformat.levels.levelfile.LevelFileReader;

public class PairReader implements Iterator<Pair<byte[], byte[]>> {

	private LevelFile level;

	private LevelFileReader reader;

	private Iterator<Pair<byte[], byte[]>> it;

	private Pair<byte[], byte[]> curr;

	public PairReader(LevelFile levelFile) throws Exception {
		this.level = levelFile;
		this.reader = levelFile.getReader();
		this.curr = getNext();
	}

	private Pair<byte[], byte[]> getNext() {
		while (it == null || !it.hasNext()) {
			if (!reader.hasNext())
				return null;
			else
				it = reader.next().iterator();
		}
		return it.next();
	}

	@Override
	public boolean hasNext() {
		return (curr != null);
	}

	@Override
	public Pair<byte[], byte[]> next() {
		Pair<byte[], byte[]> ret = curr;
		curr = getNext();
		return ret;
	}

	public Pair<byte[], byte[]> peek() {
		return curr;
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub
		
	}

}
