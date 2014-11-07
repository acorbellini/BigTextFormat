package edu.bigtextformat.levels;

import java.util.ArrayList;
import java.util.List;

import edu.bigtextformat.levels.levelfile.LevelFile;
import edu.bigtextformat.levels.levelfile.LevelFileWriter;

public class CompactWriter {
	List<LevelFile> temps = new ArrayList<>();
	LevelFile curr = null;
	LevelFileWriter currWriter = null;
	private int level;
	private SortedLevelFile file;

	public CompactWriter(SortedLevelFile file, int level) {
		this.file = file;
		this.level = level;
	}

	public void add(DataBlock db) throws Exception {
		checkNewFile();
		currWriter.add(db);
		checkSize();
	}

	private void checkNewFile() throws Exception {
		if (curr == null) {
			curr = LevelFile.newFile(file.getCwd().toString(), file.getOpts(),
					level, file.getLastLevelIndex(level));
			temps.add(curr);
			currWriter = curr.getWriter();
		}
	}

	private void checkSize() throws Exception {
		if (curr.size() > level * file.getOpts().baseSize) {
			currWriter.close();
			curr.commit();
			curr = null;
		}
	}

	public void persist() throws Exception {
		if (curr != null) {
			currWriter.close();
			curr.commit();
		}
		for (LevelFile levelFile : temps) {
			// System.out.println("Persisting " + levelFile);
			levelFile.persist();
			// System.out.println("Wrote "
			// + levelFile.print(file.getOpts().format));
			file.addLevel(levelFile);

			// System.out.println("Persisted " + levelFile);
		}

		// System.out.println("Finished Compact Writer");
	}

	public void add(byte[] k, byte[] v) throws Exception {
		checkNewFile();
		currWriter.add(k, v);
		checkSize();
	}
}