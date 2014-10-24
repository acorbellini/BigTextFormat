package edu.bigtextformat.record;

import edu.jlime.util.ByteBuffer;

public class IntegerType implements FormatType<Integer> {

	@Override
	public int compare(byte[] k1, byte[] k2) {
		Integer i1 = new ByteBuffer(k1).getInt();
		Integer i2 = new ByteBuffer(k2).getInt();
		return i1.compareTo(i2);
	}

	@Override
	public int size(int offset, byte[] d) {
		return 4;
	}

	@Override
	public byte[] getData(int offset, byte[] d) {
		return new ByteBuffer(d, offset).get(4);
	}

	@Override
	public Integer get(byte[] k) {
		return new ByteBuffer(k).getInt();
	}

	@Override
	public byte[] toBytes(Object object) {
		Integer i = (Integer) object;
		ByteBuffer b = new ByteBuffer(4);
		b.putInt(i);
		return b.build();
	}

}
