package edu.bigtextformat.block;

import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.log4j.Logger;

import edu.bigtextformat.raw.RawFile;
import edu.bigtextformat.record.DataType;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.DataTypeUtils;
import edu.jlime.util.compression.CompressionType;
import edu.jlime.util.compression.Compressor;

public class Block implements DataType<Block> {

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

	private static final int OVERHEAD = 8 + 4 + 1 + (1 + 4 + 4 + 8) + 4 + 8;

	private Logger log = Logger.getLogger(Block.class);

	private byte[] p = new byte[] {};

	long checksum;

	private Compressor comp;

	private int payloadSize;

	private long pos;
	private int effectiveSize;

	public Block(byte[] payload, Compressor comp) {
		this.comp = comp;
		this.p = payload;
	}

	public Block(long pos, int size) {
		this.pos = pos;
		this.effectiveSize = size;
	}

	@Override
	public Block fromByteArray(byte[] data) throws Exception {
		ByteBuffer buffer = new ByteBuffer(data);
		long magic = buffer.getLong();
		if (magic != BLOCK_MAGIC && magic != BLOCK_MAGIC_V2)
			throw new Exception("Invalid Block");

		int pointsToEnd = buffer.getInt();

		if (pointsToEnd != data.length)
			throw new Exception("Invalid Block Width");

		byte status = buffer.get();

		// byte[] escaped = outer.getByteArray();

		byte compType = buffer.get();
		if (compType != -1) {
			comp = CompressionType.getByID(compType);
		}

		if (magic == BLOCK_MAGIC_V2)
			this.payloadSize = buffer.getInt();

		p = buffer.getByteArray();
		checksum = buffer.getLong();

		buffer.setOffset(pointsToEnd - 4 - 8);

		int pointsToStart = buffer.getInt();

		if (pointsToEnd != pointsToStart)
			throw new Exception("Invalid Block Width");

		long endmagic = buffer.getLong();
		if (endmagic != BLOCK_MAGIC_END) {
			throw new Exception("Invalid Block End");
		}
		if (checksum != getCheckSum(p))
			throw new Exception("Different checksums");

		this.p = comp == null ? p : comp.uncompress(p, payloadSize);

		return this;
	}

	@Override
	public byte[] toByteArray() {
		int payloadSize = p.length;

		byte[] compressedPayload = p;

		if (comp != null)
			compressedPayload = comp.compress(p);

		int max = OVERHEAD + compressedPayload.length;
		ByteBuffer ret = new ByteBuffer(max);
		ret.putLong(BLOCK_MAGIC_V2); // 8
		ret.putInt(max); // Width
		ret.put((byte) 0); // 1

		if (comp != null)
			ret.put(comp.getType().getId());
		else
			ret.put((byte) -1);

		ret.putInt(payloadSize);

		ret.putByteArray(compressedPayload);// NDATA
		ret.putLong(getCheckSum(compressedPayload)); // 8

		int otherMax = ret.size() + 4 + 8;
		ret.putInt(otherMax); // 4 Width
		ret.putLong(BLOCK_MAGIC_END); // 8

		if (max != otherMax)
			log.error("This shouldn't happen.");
		if (ret.getBuffered().length != max)
			log.error("This shouldn't happen too.");
		byte[] build = ret.build();
		return build;
	}

	public long getCheckSum(byte[] b) {
		Checksum crc = new CRC32();
		crc.update(b, 0, b.length);
		return crc.getValue();
	}

	public byte[] payload() {
		return p;
	}

	public int payloadSize() {
		return p.length;
	}

	public void written(long pos, int length) {
		this.pos = pos;
		this.effectiveSize = length;
	}

	public int size() {
		return effectiveSize;
	}

	public long getNextBlockPos() {
		return pos + effectiveSize;
	}

	public long getPos() {
		return pos;
	}
}
