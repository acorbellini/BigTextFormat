package edu.bigtextformat.record;

import java.util.Date;

import edu.jlime.util.ByteBuffer;

public class DateType implements FormatType<Date> {

	@Override
	public int compare(byte[] k1, int offset1, byte[] k2, int offset2) {
		Long l1 = new ByteBuffer(k1).getLong();
		Long l2 = new ByteBuffer(k2).getLong();
		return l1.compareTo(l2);
	}

	@Override
	public Date get(byte[] k) {
		return new Date(new ByteBuffer(k).getLong());
	}

	@Override
	public byte[] getData(int offset, byte[] d) {
		return new ByteBuffer(d, offset).get(8);
	}

	@Override
	public FormatTypes getType() {
		return FormatTypes.DATE;
	}

	@Override
	public int size(int offset, byte[] d) {
		return 8;
	}

	@Override
	public byte[] toBytes(Object object) {
		Date d = (Date) object;
		ByteBuffer buff = new ByteBuffer();
		buff.putLong(d.getTime());
		return buff.build();
	}

}
