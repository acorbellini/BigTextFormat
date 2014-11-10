package edu.bigtextformat.levels;

import edu.bigtextformat.levels.levelfile.LevelFile;
import edu.bigtextformat.levels.levelfile.LevelFileReader;

public class PairReader {

	private LevelFileReader reader;

	private DataBlockIterator it;

	public PairReader(LevelFile levelFile) throws Exception {
		this.reader = levelFile.getReader();
	}

	public boolean hasNext() {
		while (it == null || !it.hasNext()) {
			if (!reader.hasNext())
				return false;
			else
				it = reader.next().iterator();
		}
		return (it.hasNext());
	}

	public void advance() {
		if (!hasNext()) {
			it = null;
			return;
		}
		it.advance();
	}

	public byte[] getKey() {
		if (it == null)
			return null;
		return it.getKey();
	}

	public byte[] getValue() {
		if (it == null)
			return null;
		return it.getVal();
	}
}
