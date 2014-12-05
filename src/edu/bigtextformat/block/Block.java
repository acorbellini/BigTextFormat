package edu.bigtextformat.block;

import java.util.zip.CRC32;
import java.util.zip.Checksum;

import javax.management.MXBean;

import edu.bigtextformat.raw.RawFile;
import edu.bigtextformat.record.DataType;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.DataTypeUtils;
import edu.jlime.util.compression.CompressionType;
import edu.jlime.util.compression.Compressor;

public class Block implements DataType<Block> {
	private static final byte[] BLOCK_MAGIC_END_AS_BYTES = "BLOCKEND"
			.getBytes();

	private static final byte[] ESCAPE_MAGIC_AS_BYTES = "ESCMAGIC".getBytes();

	private static final byte[] BLOCK_MAGIC_AS_BYTES = "BLKSTART".getBytes();

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

	private void updateMaxSize(int length) {
		if (fixed && pos == -1) {
			maxPayloadSize = length;
		} else if (length > maxPayloadSize) {
			maxPayloadSize = (int) (length * 2);
		}
	}

	public long getCheckSum(byte[] b) {
		Checksum crc = new CRC32();
		crc.update(b, 0, b.length);
		return crc.getValue();
	}

	public void setDeleted(boolean deleted) {
		this.deleted = deleted;
	}

	public void setFixed(boolean f) {
		this.fixed = f;
	}

	public void setCompressed(Compressor comp) {
		this.comp = comp;
	}

	@Override
	public byte[] toByteArray() {
		ByteBuffer buff = new ByteBuffer(1 + 4 + p.length + 8);
		if (comp != null)
			buff.put(comp.getType().getId());
		else
			buff.put((byte) -1);
		buff.putByteArray(p);// NDATA
		buff.putLong(getCheckSum(p)); // 8
		byte[] built = buff.build();

		byte[] replaced = escape(built);
		// int max = Math.max(maxPayloadSize - 8, 4 + 1 + 4 + replaced.length +
		// 4
		// + 8);

		int max = 8 + 4 + 1 + 4 + replaced.length + 4 + 8;
		if (maxPayloadSize > max)
			max = maxPayloadSize;
		
		ByteBuffer ret = new ByteBuffer(max);
		ret.putLong(BLOCK_MAGIC); // 8
		ret.putInt(max); // Points to end (except the first elements).
		ret.put(getStatus()); // 1
		ret.putByteArray(replaced); // 4 + N
		ret.padTo(maxPayloadSize - 4 - 8);
		ret.putInt(ret.size() + 4 + 8); // 4 Points to start
		ret.putLong(BLOCK_MAGIC_END); // 8
		int size = ret.getBuffered().length;

		byte[] build = ret.build();
		if (size != ret.getBuffered().length)
			System.out.println("This should not happen.");
		return build;
	}

	public ByteBuffer asByteBuffer() {
		ByteBuffer buff = new ByteBuffer(1 + 4 + p.length + 8);
		if (comp != null)
			buff.put(comp.getType().getId());
		else
			buff.put((byte) -1);
		buff.putByteArray(p);// NDATA
		buff.putLong(getCheckSum(p)); // 8
		byte[] built = buff.build();

		byte[] replaced = escape(built);
		// int max = Math.max(maxPayloadSize - 8, 4 + 1 + 4 + replaced.length +
		// 4
		// + 8);

		int max = 8 + 4 + 1 + 4 + replaced.length + 4 + 8;
		if (maxPayloadSize > max)
			max = maxPayloadSize;
		else
			System.out.println("Less than max");

		ByteBuffer ret = new ByteBuffer(max);
		ret.putLong(BLOCK_MAGIC); // 8
		ret.putInt(max); // Points to end (except the first elements).
		ret.put(getStatus()); // 1
		ret.putByteArray(replaced); // 4 + N
		ret.padTo(maxPayloadSize - 4 - 8);
		ret.putInt(ret.size() + 4 + 8); // 4 Points to start
		ret.putLong(BLOCK_MAGIC_END); // 8

		// int size = ret.getBuffered().length;
		// byte[] build = ret.build();
		// if (size != ret.getBuffered().length)
		// System.out.println("This should not happen.");
		return ret;

	}

	private static byte[] escape(byte[] built) {
		int i = check(built, 0);
		byte[] ret = built;
		if (i < built.length) {
			int last = 0;
			ByteBuffer buff = new ByteBuffer(built.length);
			while (i < built.length) {
				buff.putRawByteArray(built, last, i - last);
				buff.putLong(ESCAPE_MAGIC);
				buff.putRawByteArray(built, i, 8);
				last = i + 8;
				i = check(built, last);
			}
			buff.putRawByteArray(built, last, i - last);
			return buff.build();
		}
		return ret;
	}

	private static int checkEscape(byte[] built, int start) {
		for (int j = start; j < built.length - 8 + 1; j++) {
			boolean escape = true;
			for (int i = 0; i < 8; i++) {
				if (built[i + j] != ESCAPE_MAGIC_AS_BYTES[i]) {
					escape = false;
				}
			}
			if (escape)
				return j;
		}
		return built.length;
	}

	private static int check(byte[] built, int start) {
		for (int j = start; j < built.length - 8 + 1; j++) {
			boolean escape = true;
			boolean blockend = true;
			boolean blockstart = true;
			for (int i = 0; i < 8; i++) {
				if (built[i + j] != ESCAPE_MAGIC_AS_BYTES[i]) {
					escape = false;
				}
				if (built[i + j] != BLOCK_MAGIC_AS_BYTES[i]) {
					blockstart = false;
				}
				if (built[i + j] != BLOCK_MAGIC_END_AS_BYTES[i]) {
					blockend = false;
				}
			}
			if (escape || blockend || blockstart)
				return j;
		}
		return built.length;
	}

	private Byte getStatus() {
		byte status = (byte) ((deleted ? 0x1 : 0x0) | (fixed ? 0x2 : 0x0));
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

		byte status = outer.get();
		deleted = ((status & 0x1) == 0x1);
		fixed = ((status & 0x2) == 0x2);

		byte[] escaped = outer.getByteArray();

		outer.setOffset(pointsToEnd - 4 - 8);
		int pointsToStart = outer.getInt();
		long endmagic = outer.getLong();
		if (endmagic != BLOCK_MAGIC_END) {
			throw new Exception("Invalid Block End");
		}

		ByteBuffer buff = new ByteBuffer(unescape(escaped));
		this.maxPayloadSize = data.length;
		byte compType = buff.get();
		if (compType != -1) {
			comp = CompressionType.getByID(compType);
		}
		p = buff.getByteArray();
		checksum = buff.getLong();

		if (checksum != getCheckSum(p))
			throw new Exception("Different checksums");
		return this;
	}

	private static byte[] unescape(byte[] built) {
		int i = checkEscape(built, 0);
		byte[] ret = built;
		if (i < built.length) {
			int last = 0;
			ByteBuffer buff = new ByteBuffer(built.length);
			while (i < built.length) {
				buff.putRawByteArray(built, last, i - last);
				buff.putRawByteArray(built, i + 8, 8);
				last = i + 8 + 8;
				i = checkEscape(built, last);
			}
			buff.putRawByteArray(built, last, i - last);
			return buff.build();
		}
		return ret;

		// ByteBuffer buff = new ByteBuffer(byteArray.length);
		// for (int i = 0; i < byteArray.length;) {
		// if (i + 8 - 1 < byteArray.length
		// && DataTypeUtils.byteArrayToLong(byteArray, i) == ESCAPE_MAGIC) {
		// buff.putRawByteArray(byteArray, i + 8, 8);
		// i += 16;
		// } else {
		// buff.put(byteArray[i]);
		// i++;
		// }
		// }
		// return buff.build();
	}

	public byte[] payload() {
		if (comp != null)
			return comp.uncompress(p);
		return p;
	}

	public void setPayload(byte[] newPayload) throws Exception {
		long oldpos = pos;
		byte[] oldPayload = p;
		byte[] payload = newPayload;
		if (comp != null)
			payload = comp.compress(newPayload);

		updateMaxSize(payload.length);

		this.p = payload;
		byte[] bytes = toByteArray();
		if (pos != -1 && bytes.length > size()) {
			if (fixed) {
				this.p = oldPayload;
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

	public long getPos() {
		return pos;
	}

	public static Block read(BlockFile blockFile, long pos) throws Exception {
		RawFile raw = blockFile.getRawFile();
		int blockSize = raw.readInt(pos + 8);
		byte[] data = new byte[blockSize];
		raw.read(pos, data);
		return new Block(blockFile, pos, -1, pos + blockSize)
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

	public long size() {
		return getNextBlockPos() - getPos();
	}

	public static void main(String[] args) {
		// byte[] escape = escape("BLKSTARTESCMAGICBLOCKEND".getBytes());
		byte[] escape = escape("123BLKSTART45678ESCMAGIC9101112BLOCKEND131415"
				.getBytes());
		// byte[] escape = escape("BLKSTARTasdnasmdnsaBLOCKEND".getBytes());
		System.out.println(new String(escape));
		System.out.println(new String(unescape(escape)));
	}

	public static void setDeleted(RawFile f, byte status, long pos2)
			throws Exception {
		f.writeByte(pos2 + 8 + 4, status);
	}

	public void setMemoryMapped(boolean memoryMapped) {
		this.memoryMapped = memoryMapped;
	}

	public boolean isMemoryMapped() {
		return memoryMapped;
	}
}
