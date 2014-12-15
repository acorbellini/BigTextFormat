package edu.bigtextformat.levels;

import edu.bigtextformat.levels.datablock.DataBlockIterator;
import edu.bigtextformat.levels.levelfile.LevelFile;
import edu.bigtextformat.levels.levelfile.LevelFileReader;

public class PairReader {

	private LevelFileReader reader;

	private DataBlockIterator it;

	public PairReader(LevelFile levelFile) throws Exception {
		this.reader = levelFile.getReader();
		advanceIt();
		// if (reader.hasNext()) {
		// curr = reader.next();
		// advanceIt();
		// } else
		// curr = null;

	}

	public void advance() {
		if (it != null) {
			it.advance();
			if (!it.hasNext())
				advanceIt();
		}
		// if (!it.hasNext()) {
		// advanceIt();
		// if (it == null)
		// return;
		// }
		// else{
		// it.advance();
		// if(!it.hasNext())
		//
		// }
	}

	// while (it == null || !it.hasNext()) {
	// if (reader.hasNext())
	// it = reader.next().iterator();
	// }
	// if (!it.hasNext())
	// it = null;
	// }

	private void advanceIt() {
		try {
			while (reader.hasNext()) {
				it = reader.next().iterator();
				if (it.hasNext())
					return;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		it = null;
	}

	public byte[] getKey() {
		if (it == null)
			return null;
		return it.getKey();
	}

	public LevelFileReader getReader() {
		return reader;
	}

	public byte[] getValue() {
		if (it == null)
			return null;
		return it.getVal();
	}

	public boolean hasNext() {
		return it != null;

	}
}
