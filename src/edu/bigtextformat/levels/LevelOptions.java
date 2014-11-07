package edu.bigtextformat.levels;

import edu.bigtextformat.block.BlockFormat;

public class LevelOptions {

	public int maxBlockSize;
	public int memTableSize;
	public int maxMemTablesWriting;
	public BlockFormat format;
	public int baseSize;
	public int maxLevel0Files;
	public int maxLevelFiles;
	public int compactLevel0Threshold;

	public LevelOptions setFormat(BlockFormat format) {
		this.format = format;
		return this;
	}

	public LevelOptions setMaxMemTablesWriting(int maxMemTablesWriting) {
		this.maxMemTablesWriting = maxMemTablesWriting;
		return this;
	}

	public LevelOptions setMemTableSize(int memTableSize) {
		this.memTableSize = memTableSize;
		return this;
	}

	public LevelOptions setBaseSize(int baseSize) {
		this.baseSize = baseSize;
		return this;
	}

	public LevelOptions setMaxLevel0Files(int maxLevel0Files) {
		this.maxLevel0Files = maxLevel0Files;
		return this;
	}

	public LevelOptions setMaxLevelFiles(int maxLevelsFiles) {
		this.maxLevelFiles = maxLevelsFiles;
		return this;
	}

	public LevelOptions setMaxBlockSize(int maxBlockSize) {
		this.maxBlockSize = maxBlockSize;
		return this;
	}
	
	public LevelOptions setCompactLevel0Threshold(int compactLevel0Threshold) {
		this.compactLevel0Threshold = compactLevel0Threshold;
		return this;
	}
}
