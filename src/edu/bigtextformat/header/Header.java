package edu.bigtextformat.header;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;

import edu.bigtextformat.raw.RawFile;
import edu.bigtextformat.record.DataType;
import edu.jlime.util.ByteBuffer;

public class Header implements DataType<Header> {
	long magic = 0;
	int size;
	Map<String, byte[]> data = new HashMap<String, byte[]>();
	long crc;
	private long fsize;

	RawFile f;

	public Header(RawFile file, int size, long magic) throws IOException {
		this.f = file;
		this.magic = magic;
		this.size = size;
	}

	private Header() {
	}

	public byte[] get(String k) {
		return data.get(k);
	}

	public static Header readOrCreateHeader(RawFile file, int headerSize,
			long expectedMagic) throws Exception {
		Header h;
		if (file.length() > 0l) {
			long magic = file.readLong(0);
			if (magic != expectedMagic)
				throw new Exception("Wrong File Type");
			int size = file.readInt(0 + 8);
			byte[] headerAsBytes = file.readBytes(0 + 8 + 4, size);
			h = new Header().fromByteArray(headerAsBytes);
			if (h.crc != h.getMapCRC())
				throw new Exception("Failed Header Data CRC test");
			if (h.size != file.length())
				throw new Exception("Different file size as previouly stored");
		} else {
			h = new Header(file, headerSize, expectedMagic);
		}
		return h;
	}

	@Override
	public byte[] toByteArray() throws Exception {
		ByteBuffer buff = new ByteBuffer();
		buff.putLong(magic);
		buff.putInt(size);
		buff.putLong(f.length());

		long crc = getMapCRC();

		buff.putStringByteArrayMap(data);
		buff.putLong(crc);
		return buff.build();
	}

	private long getMapCRC() {
		ByteBuffer map = new ByteBuffer();
		map.putStringByteArrayMap(data);
		byte[] mapAsBytes = map.build();
		CRC32 crc32 = new CRC32();
		crc32.update(mapAsBytes);
		long crc = crc32.getValue();
		return crc;
	}

	@Override
	public Header fromByteArray(byte[] data) throws Exception {
		ByteBuffer buff = new ByteBuffer(data);
		this.magic = buff.getLong();
		this.size = buff.getInt();
		this.fsize = buff.getLong();
		this.data = buff.getStringByteArrayMap();
		this.crc = buff.getLong();
		return this;
	}

	public long getCrc() {
		return crc;
	}

	public long getFsize() {
		return fsize;
	}

	public String getString(String string) {
		return new String(get(string));
	}
}
