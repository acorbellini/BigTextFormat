package edu.bigtextformat.block;

import edu.jlime.util.compression.Compressor;

public class BlockFileOptions {
	int headerSize = 512;

	int minSize = 1024;

	long magic = 0L;

	Compressor comp = null;

	boolean trunc = false;

	boolean readOnly = false;

	boolean appendOnly = false;

	public BlockFileOptions setAppendOnly(boolean appendOnly) {
		this.appendOnly = appendOnly;
		return this;
	}

	public BlockFileOptions setMagic(long magic) {
		this.magic = magic;
		return this;
	}

	public BlockFileOptions setComp(Compressor comp) {
		this.comp = comp;
		return this;
	}

	public BlockFileOptions setHeaderSize(int headerSize) {
		this.headerSize = headerSize;
		return this;
	}

	public BlockFileOptions setMinSize(int minSize) {
		this.minSize = minSize;
		return this;
	}

	public BlockFileOptions setReadOnly(boolean read) {
		this.readOnly = read;
		return this;
	}

	public BlockFileOptions setTrunc(boolean trunc) {
		this.trunc = trunc;
		return this;
	}
}
