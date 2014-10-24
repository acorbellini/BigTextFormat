package edu.bigtextformat.header;

import java.util.HashMap;
import java.util.Map;

import edu.bigtextformat.block.Block;
import edu.bigtextformat.record.DataType;
import edu.jlime.util.ByteBuffer;

public class Header implements DataType<Header> {
	Block b;
	private long fsize;
	Map<String, byte[]> data = new HashMap<String, byte[]>();

	private Header(Block b, long fsize, Map<String, byte[]> data) {
		this.b = b;
		this.fsize = fsize;
		this.data = data;
	}

	private Header(Block b2) {
		this.b = b2;
	}

	public byte[] get(String k) {
		return data.get(k);
	}

	@Override
	public byte[] toByteArray() throws Exception {
		ByteBuffer buff = new ByteBuffer();
		buff.putLong(fsize);
		buff.putStringByteArrayMap(data);
		return buff.build();
	}

	@Override
	public Header fromByteArray(byte[] data) throws Exception {
		ByteBuffer buff = new ByteBuffer(data);
		this.fsize = buff.getLong();
		this.data = buff.getStringByteArrayMap();
		return this;
	}

	public long getFsize() {
		return fsize;
	}

	public String getString(String string) {
		return new String(get(string));
	}

	public static Header read(Block block) throws Exception {
		return new Header(block).fromByteArray(block.payload());
	}

	public static Header createNew(Block block) throws Exception {
		return new Header(block, block.getFile().length(),
				new HashMap<String, byte[]>());
	}

	public void setFileSize(long length) throws Exception {
		this.fsize = length;
		updateBlock();
	}

	private void updateBlock() throws Exception {
		this.b.setPayload(toByteArray());
	}

	public void putData(String k, byte[] val) throws Exception {
		this.data.put(k, val);
		updateBlock();
	}

	public static Header open(Block headerBlock) throws Exception {
		if (headerBlock.payload().length > 0)
			return read(headerBlock);
		else {
			Header createNew = createNew(headerBlock);
			createNew.updateBlock();
			return createNew;
		}
	}

	public int size() {
		return b.size();

	}
}
