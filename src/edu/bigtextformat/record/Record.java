package edu.bigtextformat.record;

import edu.bigtextformat.data.BlockData;
import edu.jlime.util.ByteBuffer;

public class Record implements BlockData {
	private RecordFormat format;
	private Object[] data;

	public Record(RecordFormat recordFormat) {
		this.format = recordFormat;
		this.data = new Object[format.size()];
	}

	public Record set(String k, Object val) {
		data[format.getPos(k)] = val;
		return this;
	}

	@Override
	public byte[] toByteArray() {
		ByteBuffer buff = new ByteBuffer();
		int i = 0;
		for (Object object : data) {
			buff.putRawByteArray(format.getFormat(i++).toBytes(object));
		}
		return buff.build();
	}

}
