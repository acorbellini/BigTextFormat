package edu.bigtextformat.record;

import edu.jlime.util.ByteBuffer;

public class LongType implements FormatType<Long> {

	@Override
	public int compare(byte[] k1, byte[] k2) {
		Long l1 = new ByteBuffer(k1).getLong();
		Long l2 = new ByteBuffer(k2).getLong();
		return l1.compareTo(l2);
	}

	@Override
	public int size(int offset, byte[] d) {
		return 8;
	}

	@Override
	public byte[] getData(int offset, byte[] d) {
		return new ByteBuffer(d, offset).get(8);
	}

	@Override
	public Long get(byte[] k) {
		return new ByteBuffer(k).getLong();
	}

}
