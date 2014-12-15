package edu.bigtextformat.block;

import edu.bigtextformat.record.RecordFormat;

public enum BlockFormats {
	RECORD(0), JSON(1), XML(2);

	public static BlockFormats get(int type) {
		for (BlockFormats f : BlockFormats.values()) {
			if (f.getID() == type)
				return f;
		}
		return null;
	}

	int id;

	private BlockFormats(int id) {
		this.id = id;
	}

	public BlockFormat fromByteArray(byte[] bs) throws Exception {
		switch (this) {
		case RECORD:
			return new RecordFormat().fromByteArray(bs);
		case JSON:
			break;
		case XML:
			break;
		default:
			break;
		}
		return null;
	}

	public int getID() {
		return id;
	}
}
