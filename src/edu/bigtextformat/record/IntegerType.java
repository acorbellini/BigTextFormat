package edu.bigtextformat.record;

import edu.jlime.util.ByteBuffer;
import edu.jlime.util.DataTypeUtils;

public class IntegerType implements FormatType<Integer> {

	@Override
	public int compare(byte[] k1, int offset1, byte[] k2, int offset2) {
		return Integer.compare(DataTypeUtils.byteArrayToInt(k1, offset1),
				DataTypeUtils.byteArrayToInt(k2, offset2));
	}

	@Override
	public Integer get(byte[] k) {
		return new ByteBuffer(k).getInt();
	}

	@Override
	public byte[] getData(int offset, byte[] d) {
		return new ByteBuffer(d, offset).get(4);
	}

	@Override
	public FormatTypes getType() {
		return FormatTypes.INTEGER;
	}

	@Override
	public int size(int offset, byte[] d) {
		return 4;
	}

	@Override
	public byte[] toBytes(Object object) {
		Integer i = (Integer) object;
		ByteBuffer b = new ByteBuffer(4);
		b.putInt(i);
		return b.build();
	}

}
