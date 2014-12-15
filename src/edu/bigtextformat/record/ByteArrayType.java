package edu.bigtextformat.record;

import edu.jlime.util.ByteBuffer;

public class ByteArrayType implements FormatType<byte[]> {

	@Override
	public int compare(byte[] k1, int offset1, byte[] k2, int offset2) {
		if (k1.length > k2.length)
			return 1;
		else if (k1.length < k2.length)
			return -1;

		for (int i = 0; i < k1.length; i++) {
			if (k1[i] > k2[i])
				return 1;
			else if (k1[i] < k2[i])
				return -1;
		}
		return 0;
	}

	@Override
	public byte[] get(byte[] k) {
		return k;
	}

	@Override
	public byte[] getData(int offset, byte[] d) {
		ByteBuffer buff = new ByteBuffer(d, offset);
		return buff.getByteArray();
	}

	@Override
	public FormatTypes getType() {
		return FormatTypes.BYTEARRAY;
	}

	@Override
	public int size(int offset, byte[] d) {
		ByteBuffer buff = new ByteBuffer(d, offset);
		return 4 + buff.getInt();
	}

	@Override
	public byte[] toBytes(Object object) {
		byte[] el = (byte[]) object;
		ByteBuffer buff = new ByteBuffer();
		buff.putByteArray(el);
		return buff.build();
	}

}
