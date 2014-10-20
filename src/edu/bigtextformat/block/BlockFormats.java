package edu.bigtextformat.block;

import edu.bigtextformat.record.RecordFormat;

public enum BlockFormats {
	RECORD, JSON, XML;

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
}
