package edu.bigtextformat.levels;

import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.block.BlockFormats;
import edu.bigtextformat.record.DataType;
import edu.jlime.util.ByteBuffer;

public class LevelOptions implements DataType<LevelOptions> {

	public int maxBlockSize = 64 * 1024; // 64k
	public int memTableSize = 512 * 1024; // 512k
	public int maxMemTablesWriting = 5;
	public int baseSize = 512 * 1024; // 512k
	public int maxLevel0Files = 15;
	public int maxLevelFiles = 20;
	public int compactLevel0Threshold = 2;

	public BlockFormat format;

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

	@Override
	public byte[] toByteArray() throws Exception {
		ByteBuffer buff = new ByteBuffer();
		buff.putInt(maxBlockSize);
		buff.putInt(memTableSize);
		buff.putInt(maxMemTablesWriting);
		buff.putInt(baseSize);
		buff.putInt(maxLevel0Files);
		buff.putInt(maxLevelFiles);
		buff.putInt(compactLevel0Threshold);
		buff.putInt(format.getType().getID());
		buff.putByteArray(format.toByteArray());
		return buff.build();
	}

	@Override
	public LevelOptions fromByteArray(byte[] data) throws Exception {
		ByteBuffer buff = new ByteBuffer(data);
		maxBlockSize = buff.getInt();
		memTableSize = buff.getInt();
		maxMemTablesWriting = buff.getInt();
		baseSize = buff.getInt();
		maxLevel0Files = buff.getInt();
		maxLevelFiles = buff.getInt();
		compactLevel0Threshold = buff.getInt();
		int type = buff.getInt();
		format = BlockFormat.getFormat(BlockFormats.get(type),
				buff.getByteArray());
		return this;
	}
}
