package edu.bigtextformat.data;

import java.util.ArrayList;

import edu.bigtextformat.Range;
import edu.bigtextformat.block.Block;
import edu.bigtextformat.record.DataType;
import edu.jlime.util.ByteBuffer;

public class DataPayload implements DataType<DataPayload> {

	Block b;

	int level = 0;

	private static final int LENGTH_FIELD_SIZE = 4;

	private ArrayList<byte[]> records = new ArrayList<>();

	public void add(byte[] firstRecord) {
		this.records.add(firstRecord);
	}

	public int size() {
		int ret = 0;
		for (byte[] bs : records)
			ret += bs.length + LENGTH_FIELD_SIZE;
		return ret;
	}

	@Override
	public byte[] toByteArray() {
		ByteBuffer buff = new ByteBuffer();
		for (byte[] bs : records)
			buff.putByteArray(bs);
		return buff.build();
	}

	@Override
	public DataPayload fromByteArray(byte[] data) {
		DataPayload p = new DataPayload();
		ByteBuffer buff = new ByteBuffer(data);
		while (buff.hasRemaining())
			p.add(buff.getByteArray());
		return p;
	}

	public boolean isEmpty() {
		return records.isEmpty();
	}

	public byte[] first() {
		return records.get(0);
	}

	public byte[] last() {
		return records.get(records.size() - 1);
	}

	public DataPayload() {
	}

	public DataPayload split() {
		DataPayload half = new DataPayload();
		for (int i = 0; i < records.size() / 2; i++) {
			half.add(records.remove(i));
		}
		if (half.isEmpty())
			return null;
		return half;
	}

	public Range range() {
		return new Range(first(), last());
	}

	public static DataPayload open(Block b2) {
		// TODO Auto-generated method stub
		return null;
	}
}
