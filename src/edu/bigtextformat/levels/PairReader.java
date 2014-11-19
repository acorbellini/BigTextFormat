package edu.bigtextformat.levels;

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

	private void advanceIt() {
		while (reader.hasNext()) {
			it = reader.next().iterator();
			if (it.hasNext())
				return;
		}
		it = null;
	}

	// while (it == null || !it.hasNext()) {
	// if (reader.hasNext())
	// it = reader.next().iterator();
	// }
	// if (!it.hasNext())
	// it = null;
	// }

	public boolean hasNext() {
		return it != null;

	}

	public void advance() {
		if(it!=null){
			it.advance();
			if(!it.hasNext())
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
