package edu.bigtextformat.levels;

import edu.bigtextformat.levels.compactor.Writer;
import edu.bigtextformat.levels.levelfile.LevelFile;
import edu.bigtextformat.levels.levelfile.LevelFileWriter;

public class SingleFileWriter implements Writer {
	private LevelFile levelFile;
	private LevelFileWriter lwriter;
	private Level level;

	public SingleFileWriter(Level level) throws Exception {
		this.level = level;
		this.levelFile = LevelFile.newFile(level.getFile().getCwd().getPath(),
				level.getOpts(), level.level(), level.getLastLevelIndex());
		this.lwriter = levelFile.getWriter();
	}

	@Override
	public void add(DataBlock dataBlock) throws Exception {
		lwriter.add(dataBlock);
	}

	@Override
	public void persist() throws Exception {
		lwriter.close();
		levelFile.commitAndPersist();
		level.add(levelFile);
	}

	@Override
	public void add(byte[] k, byte[] v) throws Exception {
		lwriter.add(k, v);
	}

}
