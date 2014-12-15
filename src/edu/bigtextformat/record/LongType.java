package edu.bigtextformat.record;

import edu.jlime.util.ByteBuffer;

public class LongType implements FormatType<Long> {

	@Override
	public int compare(byte[] k1, int offset1, byte[] k2, int offset2) {
		return Long.compare(new ByteBuffer(k1).setOffset(offset1).getLong(),
				new ByteBuffer(k2).setOffset(offset2).getLong());
	}

	@Override
	public Long get(byte[] k) {
		return new ByteBuffer(k).getLong();
	}

	@Override
	public byte[] getData(int offset, byte[] d) {
		return new ByteBuffer(d).setOffset(offset).get(8);
	}

	@Override
	public FormatTypes getType() {
		return FormatTypes.LONG;
	}

	@Override
	public int size(int offset, byte[] d) {
		return 8;
	}

	@Override
	public byte[] toBytes(Object object) {
		Long i = (Long) object;
		ByteBuffer b = new ByteBuffer(8);
		b.putLong(i);
		return b.build();
	}

}
