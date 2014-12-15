package edu.bigtextformat.record;

import edu.jlime.util.ByteBuffer;

public class StringType implements FormatType<String> {

	@Override
	public int compare(byte[] k1, int offset1, byte[] k2, int offset2) {
		String s = new String(k1);
		String s2 = new String(k2);
		return s.compareTo(s2);
	}

	@Override
	public String get(byte[] k) {
		return new String(k);
	}

	@Override
	public byte[] getData(int offset, byte[] d) {
		ByteBuffer buff = new ByteBuffer(d, offset);
		int size = buff.getInt();
		return buff.get(size);
	}

	@Override
	public FormatTypes getType() {
		return FormatTypes.STRING;
	}

	@Override
	public int size(int offset, byte[] d) {
		ByteBuffer buff = new ByteBuffer(d, offset);
		return 4 + buff.getInt();
	}

	@Override
	public byte[] toBytes(Object object) {
		String s = (String) object;
		ByteBuffer buff = new ByteBuffer();
		buff.putString(s);
		return buff.build();
	}

}
