package edu.bigtextformat.block;

import java.util.zip.CRC32;
import java.util.zip.Checksum;

import edu.bigtextformat.raw.RawFile;
import edu.bigtextformat.record.DataType;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.DataTypeUtils;
import edu.jlime.util.compression.CompressionType;
import edu.jlime.util.compression.Compressor;

public class Block implements DataType<Block> {

	public static Block create(BlockFile blockFile, int minSize)
			throws Exception {
		return new Block(blockFile, -1, minSize, -1);
	}

	public static Block read(BlockFile blockFile, long pos) throws Exception {
		RawFile raw = blockFile.getRawFile();
		int blockSize = raw.readInt(pos + 8);
		byte[] data = new byte[blockSize];
		raw.read(pos, data);
		return new Block(blockFile, pos, 16, pos + blockSize)
				.fromByteArray(data);
	}

	public static void setDeleted(RawFile f, byte status, long pos2)
			throws Exception {
		f.writeByte(pos2 + 8 + 4, status);
	}

	private static final byte[] BLOCK_MAGIC_END_AS_BYTES = "BLOCKEND"
			.getBytes();
	private static final byte[] ESCAPE_MAGIC_AS_BYTES = "ESCMAGIC".getBytes();
	private static final byte[] BLOCK_MAGIC_AS_BYTES = "BLKSTART".getBytes();

	private static final long BLOCK_MAGIC_V2 = DataTypeUtils
			.byteArrayToLong("BLKSTAV2".getBytes());

	private static final long BLOCK_MAGIC = DataTypeUtils
			.byteArrayToLong(BLOCK_MAGIC_AS_BYTES);

	private static final long ESCAPE_MAGIC = DataTypeUtils
			.byteArrayToLong(ESCAPE_MAGIC_AS_BYTES);
	private static final long BLOCK_MAGIC_END = DataTypeUtils
			.byteArrayToLong(BLOCK_MAGIC_END_AS_BYTES);
	long pos = -1;

	BlockFile file;

	private boolean deleted = false;
	private boolean fixed = false;

	private boolean memoryMapped = false;

	private byte[] p = new byte[] {};

	long checksum;

	private int maxPayloadSize;

	private long nextBlockPos;

	private java.nio.ByteBuffer mappedBuffer;

	private Compressor comp;
	private int origSize = 128;

	public Block(BlockFile blockFile, long pos, int minSize, long next) {
		this.pos = pos;
		this.nextBlockPos = next;
		this.maxPayloadSize = minSize;
		this.file = blockFile;
	}

	@Override
	public Block fromByteArray(byte[] data) throws Exception {
		ByteBuffer buffer = new ByteBuffer(data);
		long magic = buffer.getLong();
		if (magic != BLOCK_MAGIC && magic != BLOCK_MAGIC_V2)
			throw new Exception("Invalid Block");

		int pointsToEnd = buffer.getInt();
		byte status = buffer.get();
		deleted = ((status & 0x1) == 0x1);
		fixed = ((status & 0x2) == 0x2);

		// byte[] escaped = outer.getByteArray();

		this.maxPayloadSize = data.length;
		byte compType = buffer.get();
		if (compType != -1) {
			comp = CompressionType.getByID(compType);
		}

		if (magic == BLOCK_MAGIC_V2)
			this.origSize = buffer.getInt();

		p = buffer.getByteArray();
		checksum = buffer.getLong();

		buffer.setOffset(pointsToEnd - 4 - 8);

		int pointsToStart = buffer.getInt();
		long endmagic = buffer.getLong();
		if (endmagic != BLOCK_MAGIC_END) {
			throw new Exception("Invalid Block End");
		}
		if (checksum != getCheckSum(p))
			throw new Exception("Different checksums");
		return this;
	}

	public long getCheckSum(byte[] b) {
		Checksum crc = new CRC32();
		crc.update(b, 0, b.length);
		return crc.getValue();
	}

	public BlockFile getFile() {
		return file;
	}

	public int getMinSize() {
		return maxPayloadSize;
	}

	public long getNextBlockPos() {
		return nextBlockPos;
	}

	public long getPos() {
		return pos;
	}

	private Byte getStatus() {
		byte status = (byte) ((deleted ? 0x1 : 0x0) | (fixed ? 0x2 : 0x0));
		return status;
	}

	public boolean isDeleted() {
		return deleted;
	}

	public boolean isFixed() {
		return fixed;
	}

	public boolean isMemoryMapped() {
		return memoryMapped;
	}

	public byte[] payload() {
		if (comp != null)
			return comp.uncompress(p, origSize);
		return p;
	}

	public int payloadSize() {
		if (p == null)
			return 0;
		return p.length;
	}

	public void persist(byte[] bytes) throws Exception {
		if (pos == -1) {
			long pos = file.reserve(bytes.length);
			setPos(pos, pos + bytes.length);
		}
		if (!memoryMapped)
			file.writeBlock(pos, this, bytes);
		else {
			if (mappedBuffer == null) {
				mappedBuffer = file.getRawFile().memMap(pos, nextBlockPos);
			}
			mappedBuffer.clear();
			mappedBuffer.put(bytes);
		}
	}

	public void setCompressed(Compressor comp) {
		this.comp = comp;
	}

	public void setDeleted(boolean deleted) {
		this.deleted = deleted;
	}

	public void setFixed(boolean f) {
		this.fixed = f;
	}

	public void setMemoryMapped(boolean memoryMapped) {
		this.memoryMapped = memoryMapped;
	}

	public void setPayload(byte[] newPayload) throws Exception {
		long oldpos = pos;
		byte[] oldPayload = p;
		int origSizeOld = origSize;
		byte[] payload = newPayload;
		this.origSize = payload.length;
		if (comp != null)
			payload = comp.compress(newPayload);

		updateMaxSize(payload.length);

		this.p = payload;
		byte[] bytes = toByteArray();
		if (pos != -1 && bytes.length > size()) {
			if (fixed) {
				this.p = oldPayload;
				this.origSize = origSizeOld;
				throw new Exception("Can't expand fixed block from " + size()
						+ " to " + bytes.length);
			}
			file.removeBlock(oldpos, size(), (byte) (getStatus() | 0x1));
			this.pos = -1;
		}

		persist(bytes);

		if (oldpos != -1 && pos != oldpos) {
			file.notifyPosChanged(this, oldpos);
		}

	}

	public void setPos(long pos, long next) {
		this.pos = pos;
		this.nextBlockPos = next;
	}

	public long size() {
		return getNextBlockPos() - getPos();
	}

	@Override
	public byte[] toByteArray() {

		int max = 8 + 4 + 1 + (1 + 4 + 4 + p.length + 8) + 4 + 8;
		if (maxPayloadSize > max)
			max = maxPayloadSize;

		ByteBuffer ret = new ByteBuffer(max);
		ret.putLong(BLOCK_MAGIC_V2); // 8
		ret.putInt(max); // Points to end (except the first elements).
		ret.put(getStatus()); // 1

		if (comp != null)
			ret.put(comp.getType().getId());
		else
			ret.put((byte) -1);

		ret.putInt(origSize);

		ret.putByteArray(p);// NDATA
		ret.putLong(getCheckSum(p)); // 8

		int pad = maxPayloadSize - 4 - 8;
		ret.padTo(pad);
		ret.putInt(ret.size() + 4 + 8); // 4 Points to start
		ret.putLong(BLOCK_MAGIC_END); // 8

		byte[] build = ret.build();
		return build;
	}

	private void updateMaxSize(int length) {
		if (fixed && pos == -1) {
			maxPayloadSize = length;
		} else if (length > maxPayloadSize) {
			maxPayloadSize = (int) (length * 2);
		}
	}
}
