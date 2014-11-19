package edu.bigtextformat.levels.compactor;

import java.util.ArrayList;
import java.util.List;

import edu.bigtextformat.levels.DataBlock;
import edu.bigtextformat.levels.Level;
import edu.bigtextformat.levels.levelfile.LevelFile;
import edu.bigtextformat.levels.levelfile.LevelFileWriter;

public class CompactWriterV2 {
	List<LevelFile> temps = new ArrayList<>();
	LevelFile curr = null;
	LevelFileWriter currWriter = null;
	private Level level;

	public CompactWriterV2(Level to) {
		this.level = to;
	}

	public void add(DataBlock dataBlock) throws Exception {
		// DataBlockIterator it = db.iterator();
		// while (it.hasNext()) {
		// it.advance();
		// add(it.getKey(), it.getVal());
		// }
		checkNewFile();
		currWriter.add(dataBlock);
		checkSize();
	}

	private void checkNewFile() throws Exception {
		if (curr == null) {
			curr = LevelFile.newFile(level.getCwd().toString(),
					level.getOpts(), level.level(), level.getLastLevelIndex());
			temps.add(curr);
			currWriter = curr.getWriter();
		}
	}

	private void checkSize() throws Exception {
		if (curr.size() > Math.min(level.getOpts().maxSize,
				((level.level() / (float) level.getOpts().sizeModifier) + 1)
						* level.getOpts().baseSize)) {
			flush();
		}
	}

	private void flush() throws Exception {
		currWriter.close();
		curr.commit();
		curr = null;
	}

	public void persist() throws Exception {
		if (curr != null) {
			currWriter.close();
			curr.commit();
		}
		for (LevelFile levelFile : temps) {
			levelFile.persist();
			level.add(levelFile);
		}
	}

	public void add(byte[] k, byte[] v) throws Exception {
		checkNewFile();
		currWriter.add(k, v);
		checkSize();
	}
}