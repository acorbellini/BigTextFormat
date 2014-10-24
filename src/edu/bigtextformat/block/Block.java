package edu.bigtextformat.block;

import java.util.zip.CRC32;

import edu.bigtextformat.raw.RawFile;
import edu.bigtextformat.record.DataType;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.DataTypeUtils;
import edu.jlime.util.compression.Compression;

public class Block implements DataType<Block> {

	private static final long BLOCK_MAGIC = DataTypeUtils
			.byteArrayToLong("BLKSTART".getBytes());
	private static final long ESCAPE_MAGIC = DataTypeUtils
			.byteArrayToLong("ESCMAGIC".getBytes());
	private static final long BLOCK_MAGIC_END = DataTypeUtils
			.byteArrayToLong("BLOCKEND".getBytes());;
	long pos = -1;
	BlockFile file;
	private boolean deleted = false;
	private boolean fixed = false;
	private boolean compressed = false;

	private byte[] p = new byte[] {};
	long checksum;
	private int maxPayloadSize;
	private long nextBlockPos;

	// BLOCKSTART+TOTAL_BLOCK_SIZE + STATUS + PAYLOAD_SIZE + PAYLOAD (0) + CRC +
	// PAD (0) +
	// TOTAL_BLOCK_SIZE + BLOCKEND

	public void setPos(long pos, long next) {
		this.pos = pos;
		this.nextBlockPos = next;
	}

	public Block(BlockFile blockFile, long pos, int minSize, long next) {
		this.pos = pos;
		this.nextBlockPos = next;
		this.maxPayloadSize = minSize;
		this.file = blockFile;
	}

	public int getMinSize() {
		return maxPayloadSize;
	}

	private void updateMaxSize() {
		if (p.length > maxPayloadSize) {
			maxPayloadSize = (int) (p.length * 1.5);
		}
	}

	public long getCheckSum(byte[] b) {
		CRC32 crc = new CRC32();
		crc.update(b);
		return crc.getValue();
	}

	public void setDeleted(boolean deleted) {
		this.deleted = deleted;
	}

	public void setFixed(boolean f) {
		this.fixed = f;
	}

	public void setCompressed(boolean compressed) {
		this.compressed = compressed;
	}

	@Override
	public byte[] toByteArray() {
		ByteBuffer buff = new ByteBuffer();
		buff.put(getStatus()); // 1
		buff.putByteArray(p);// NDATA
		buff.putLong(getCheckSum(p)); // 8
		byte[] built = buff.build();

		ByteBuffer ret = new ByteBuffer();
		ret.putLong(BLOCK_MAGIC); // 8
		byte[] replaced = escape(built);
		ret.putInt(Math
				.max(maxPayloadSize - 8, 4 + 4 + replaced.length + 4 + 8)); // Points
																			// to
																			// end
		ret.putByteArray(replaced); // 4 + N
		ret.padTo(maxPayloadSize - 4 - 8);
		ret.putInt(ret.size() + 4 + 8); // 4 Points to start
		ret.putLong(BLOCK_MAGIC_END); // 8
		byte[] build = ret.build();
		return build;
	}

	private static byte[] escape(byte[] built) {
		ByteBuffer ret = new ByteBuffer(built.length);
		for (int i = 0; i < built.length;) {
			if (check(built, i)) {
				ret.putLong(ESCAPE_MAGIC);
				ret.putRawByteArray(built, i, 8);
				i += 8;
			} else {
				ret.put(built[i]);
				i++;
			}
		}

		return ret.build();
	}

	private static boolean check(byte[] built, int i) {
		if (i + 8 - 1 >= built.length)
			return false;
		long current = DataTypeUtils.byteArrayToLong(built, i);
		return current == BLOCK_MAGIC || current == BLOCK_MAGIC_END
				|| current == ESCAPE_MAGIC;
	}

	private Byte getStatus() {
		byte status = (byte) ((deleted ? 0x1 : 0x0) | (fixed ? 0x2 : 0x0) | (compressed ? 0x4
				: 0x0));
		return status;
	}

	@Override
	public Block fromByteArray(byte[] data) throws Exception {
		ByteBuffer outer = new ByteBuffer(data);
		long magic = outer.getLong();
		if (magic != BLOCK_MAGIC) {
			throw new Exception("Invalid Block");
		}
		int pointsToEnd = outer.getInt();
		byte[] escaped = outer.getByteArray();

		outer.setOffset(8 + pointsToEnd - 4 - 8);
		int pointsToStart = outer.getInt();
		long endmagic = outer.getLong();
		if (endmagic != BLOCK_MAGIC_END) {
			throw new Exception("Invalid Block End");
		}

		ByteBuffer buff = new ByteBuffer(unescape(escaped));
		this.maxPayloadSize = data.length;
		byte status = buff.get();
		deleted = ((status & 0x1) == 0x1);
		fixed = ((status & 0x2) == 0x2);
		compressed = ((status & 0x4) == 0x4);
		p = buff.getByteArray();
		checksum = buff.getLong();

		if (checksum != getCheckSum(p))
			throw new Exception("Different checksums");
		return this;
	}

	private static byte[] unescape(byte[] byteArray) {
		ByteBuffer buff = new ByteBuffer(byteArray.length);
		for (int i = 0; i < byteArray.length;) {
			if (i + 8 - 1 < byteArray.length
					&& DataTypeUtils.byteArrayToLong(byteArray, i) == ESCAPE_MAGIC) {
				buff.putRawByteArray(byteArray, i + 8, 8);
				i += 16;
			} else {
				buff.put(byteArray[i]);
				i++;
			}
		}
		return buff.build();
	}

	public byte[] payload() {
		if (compressed)
			return Compression.bzipdecompress(p);
		return p;
	}

	public void setPayload(byte[] newPayload) throws Exception {
		byte[] oldPayload = p;
		byte[] payload = newPayload;
		if (compressed)
			payload = Compression.bzipcompress(newPayload);
		this.p = payload;
		byte[] bytes = toByteArray();
		if (pos != -1 && bytes.length > size()) {
			if (fixed) {
				this.p = oldPayload;
				throw new Exception("Can't expand fixed block from " + size()
						+ " to " + bytes.length);
			}
			this.p = oldPayload;
			setDeleted(true);
			persist(toByteArray());
			file.removeBlock(this.pos);
			this.pos = -1;
			this.p = payload;
		}
		updateMaxSize();
		setDeleted(false);
		persist(bytes);
	}

	public void persist(byte[] bytes) throws Exception {
		if (pos == -1) {
			pos = file.reserve(bytes.length);
			setPos(pos, pos + bytes.length);
		}
		file.writeBlock(pos, this, bytes);
	}

	public long getPos() {
		return pos;
	}

	public static Block read(BlockFile blockFile, long pos) throws Exception {
		RawFile raw = blockFile.getFile();
		int blockSize = raw.readInt(pos + 8);
		byte[] data = new byte[blockSize + 8];
		raw.read(pos, data);
		return new Block(blockFile, pos, -1, 8 + pos + blockSize)
				.fromByteArray(data);
	}

	public boolean isDeleted() {
		return deleted;
	}

	public boolean isFixed() {
		return fixed;
	}

	public int payloadSize() {
		if (p == null)
			return 0;
		return p.length;
	}

	public static Block create(BlockFile blockFile, int minSize)
			throws Exception {
		return new Block(blockFile, -1, minSize, -1);
	}

	public BlockFile getFile() {
		return file;
	}

	public long getNextBlockPos() {
		return nextBlockPos;
	}

	public int size() {
		return (int) (getNextBlockPos() - getPos());
	}

	public static void main(String[] args) {
		byte[] escape = escape("BLKSTARTESCMAGICBLOCKEND".getBytes());
		System.out.println(new String(escape));
		System.out.println(new String(unescape(escape)));
	}
}
