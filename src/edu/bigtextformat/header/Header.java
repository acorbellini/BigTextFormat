package edu.bigtextformat.header;

import java.util.HashMap;
import java.util.Map;

import edu.bigtextformat.block.Block;
import edu.bigtextformat.record.DataType;
import edu.jlime.util.ByteBuffer;

public class Header implements DataType<Header> {
	Map<String, byte[]> data = null;

	public Header(Map<String, byte[]> map) {
		this.data = map;
	}

	public Header() {
		this(new HashMap<String, byte[]>());
	}

	public static Header open(Block headerBlock) throws Exception {
		Header ret = null;
		if (headerBlock.payload().length > 0)
			ret = read(headerBlock);
		else {
			ret = new Header();
		}
		ret.setSize(headerBlock.size());
		return ret;
	}

	public static Header read(Block block) throws Exception {
		return new Header().fromByteArray(block.payload());
	}

	private long fsize = 0;

	private long size;

	@Override
	public Header fromByteArray(byte[] data) throws Exception {
		ByteBuffer buff = new ByteBuffer(data);
		this.fsize = buff.getLong();
		this.data = buff.getStringByteArrayMap();
		return this;
	}

	public byte[] get(String k) {
		return data.get(k);
	}

	public String getString(String string) {
		return new String(get(string));
	}

	// public void putData(String k, byte[] val) throws Exception {
	// this.data.put(k, val);
	// }

	//
	// public void setFileSize(long length) throws Exception {
	// this.fsize = length;
	// // updateBlock();
	// }

	// public long size() {
	// return b != null ? b.size() : 0;
	// }

	@Override
	public byte[] toByteArray() throws Exception {
		ByteBuffer buff = new ByteBuffer();
		buff.putLong(fsize);
		buff.putStringByteArrayMap(data);
		return buff.build();
	}

	public void setSize(long size) {
		this.size = size;
	}

	public long getSize() {
		return size;
	}

	// private void updateBlock() throws Exception {
	// this.b.setPayload(toByteArray());
	// }
}
