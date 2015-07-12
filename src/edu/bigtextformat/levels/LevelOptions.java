package edu.bigtextformat.levels;

import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.block.BlockFormats;
import edu.bigtextformat.record.DataType;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.compression.CompressionType;
import edu.jlime.util.compression.Compressor;

public class LevelOptions implements DataType<LevelOptions> {
	public BlockFormat format;

	public Compressor comp = CompressionType.SNAPPY.getComp();

	// Memtable Options
	public int memTableSize = 512 * 1024; // 512k
	public int maxMemtableSegments = 4;
	public int maxLevel0WriterThreads = 4;

	// Block and File Size
	public int baseSize = 512 * 1024; // 512k
	public int maxBlockSize = 8 * 1024; // 64k
	public int maxSegmentWriters = 2;

	// Level Options
	public int maxLevel0Files = 4;
	public int maxLevelFiles = 10;
	public int compactLevel0Threshold = 4;

	// Compacting Options
	public int intersectSplit = 10;
	public int maxMergeElements = 4;
	public int maxCompactorThreads = 4;
	public int maxCompactionWriters = 4;
	public int compactTrottle = (int) (10 * 1024 * 1024f / 1000); // 10MB per
																	// sec

	// Recovery Options
	public int recoveryWriters = 10;
	public int recoveryThreads = 10;

	public int recoveryMaxIntersect = 100;

	public boolean syncmem = false;

	@Override
	public LevelOptions fromByteArray(byte[] data) throws Exception {
		ByteBuffer buff = new ByteBuffer(data);
		maxBlockSize = buff.getInt();
		memTableSize = buff.getInt();
		maxSegmentWriters = buff.getInt();
		baseSize = buff.getInt();
		maxLevel0Files = buff.getInt();
		maxLevelFiles = buff.getInt();
		compactLevel0Threshold = buff.getInt();
		maxCompactorThreads = buff.getInt();
		int type = buff.getInt();
		format = BlockFormat.getFormat(BlockFormats.get(type),
				buff.getByteArray());
		byte compType = buff.get();
		if (compType != -1)
			this.comp = CompressionType.getByID(compType);
		return this;
	}

	public LevelOptions setBaseSize(int baseSize) {
		this.baseSize = baseSize;
		return this;
	}

	public LevelOptions setCompactLevel0Threshold(int compactLevel0Threshold) {
		this.compactLevel0Threshold = compactLevel0Threshold;
		return this;
	}

	public LevelOptions setCompressed(Compressor comp) {
		this.comp = comp;
		return this;
	}

	public LevelOptions setFormat(BlockFormat format) {
		this.format = format;
		return this;
	}

	public LevelOptions setMaxBlockSize(int maxBlockSize) {
		this.maxBlockSize = maxBlockSize;
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

	public LevelOptions setSegmentWriters(int maxMemTablesWriting) {
		this.maxSegmentWriters = maxMemTablesWriting;
		return this;
	}

	public LevelOptions setMemTableSize(int memTableSize) {
		this.memTableSize = memTableSize;
		return this;
	}

	public LevelOptions setCompactTrottle(int compactTrottle) {
		this.compactTrottle = compactTrottle;
		return this;
	}

	public LevelOptions setIntersectSplit(int intersectSplit) {
		this.intersectSplit = intersectSplit;
		return this;
	}

	public LevelOptions setMaxMemtableSegments(int maxMemtableSegments) {
		this.maxMemtableSegments = maxMemtableSegments;
		return this;
	}

	public LevelOptions setMaxLevel0WriterThreads(int maxLevel0WriterThreads) {
		this.maxLevel0WriterThreads = maxLevel0WriterThreads;
		return this;
	}

	public LevelOptions setMaxCompactorThreads(int maxCompactorThreads) {
		this.maxCompactorThreads = maxCompactorThreads;
		return this;
	}

	public LevelOptions setMaxMergeElements(int maxMergeElements) {
		this.maxMergeElements = maxMergeElements;
		return this;
	}

	public LevelOptions setMaxCompactionWriters(int maxCompactionWriters) {
		this.maxCompactionWriters = maxCompactionWriters;
		return this;
	}

	public LevelOptions setRecoveryThreads(int recoveryThreads) {
		this.recoveryThreads = recoveryThreads;
		return this;
	}

	public LevelOptions setRecoveryWriters(int recoveryWriters) {
		this.recoveryWriters = recoveryWriters;
		return this;
	}

	public LevelOptions setRecoveryMaxIntersect(int recoveryMaxIntersect) {
		this.recoveryMaxIntersect = recoveryMaxIntersect;
		return this;
	}

	@Override
	public byte[] toByteArray() throws Exception {
		ByteBuffer buff = new ByteBuffer();
		buff.putInt(maxBlockSize);
		buff.putInt(memTableSize);
		buff.putInt(maxSegmentWriters);
		buff.putInt(baseSize);
		buff.putInt(maxLevel0Files);
		buff.putInt(maxLevelFiles);
		buff.putInt(compactLevel0Threshold);
		buff.putInt(maxCompactorThreads);

		buff.putInt(format.getType().getID());
		buff.putByteArray(format.toByteArray());
		if (comp != null)
			buff.put(comp.getType().getId());
		else
			buff.put((byte) -1);
		return buff.build();
	}

	public LevelOptions setSyncMemtable(boolean b) {
		this.syncmem = b;
		return this;
	}
}
