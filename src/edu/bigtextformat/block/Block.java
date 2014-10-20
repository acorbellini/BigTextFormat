package edu.bigtextformat.block;

import java.util.zip.CRC32;

import edu.bigtextformat.record.DataType;
import edu.jlime.util.ByteBuffer;

public class Block implements DataType<Block> {

	int blockLenght;
	private byte status = 0;
	private byte[] p;
	long checksum;

	private static final int HEADER_SIZE = 4 + 1 + 0 + 8;

	public Block(int blockSize) {
		this(blockSize, null);
	}

	public Block(int blockSize, byte[] payload) {
		this.blockLenght = blockSize;
		this.p = payload;
	}

	public int size() {
		return HEADER_SIZE + p.length;
	}

	public long getCheckSum(byte[] b) {
		CRC32 crc = new CRC32();
		crc.update(b);
		return crc.getValue();
	}

	public void setDeleted() {
		this.setStatus((byte) (status & 0x1));
	}

	@Override
	public byte[] toByteArray() {
		ByteBuffer buff = new ByteBuffer();
		int max = Math.max(blockLenght, size());
		buff.putInt(max);
		buff.put(status);
		buff.putByteArray(p);
		buff.putLong(getCheckSum(p));
		buff.putInt(max);
		return buff.build();
	}

	@Override
	public Block fromByteArray(byte[] data) throws Exception {
		ByteBuffer buff = new ByteBuffer(data);
		blockLenght = buff.getInt();
		setStatus(buff.get());
		p = buff.getByteArray();
		checksum = buff.getLong();
		if (checksum != getCheckSum(p))
			throw new Exception("Different checksums");
		return this;
	}

	public byte getStatus() {
		return status;
	}

	public void setStatus(byte status) {
		this.status = status;
	}

	public byte[] payload() {
		return p;
	}

}
