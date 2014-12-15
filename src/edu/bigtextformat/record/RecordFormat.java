package edu.bigtextformat.record;

import java.util.Arrays;
import java.util.List;

import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.block.BlockFormats;
import edu.bigtextformat.data.BlockData;
import edu.jlime.util.ByteBuffer;

public class RecordFormat extends BlockFormat {
	public static RecordFormat create(String[] types,
			FormatType<?>[] formatTypes, String[] k) {
		return new RecordFormat(Arrays.asList(types),
				Arrays.asList(formatTypes), Arrays.asList(k));
	}
	String[] header;
	FormatType<?>[] formats;
	private String[] key;

	private BlockFormat secondFormat;

	public RecordFormat() {
	}

	public RecordFormat(List<String> h, List<FormatType<?>> f, List<String> k) {
		this(h.toArray(new String[] {}), f.toArray(new FormatType<?>[] {}), k
				.toArray(new String[] {}));

	}

	public RecordFormat(String[] h, FormatType<?>[] f, String[] k) {
		this.header = h;
		this.formats = f;
		this.key = k;
	}

	public void addKey(BlockFormat keyFormat) {
		secondFormat = keyFormat;
	}

	@Override
	public int compare(byte[] d1, byte[] d2) {
		for (int i = 0; i < header.length; i++) {
			String k = header[i];
			int c = compare(k, d1, d2);
			if (c != 0)
				return c;
		}
		if (secondFormat != null)
			return secondFormat.compare(d1, d2);
		return 0;
	}

	private int compare(String k, byte[] d1, byte[] d2) {
		int pos = headerPos(k);

		FormatType<?> f = formats[pos];
		return f.compare(d1, getOffset(k, d1), d2, getOffset(k, d2));
	}

	@Override
	public RecordFormat fromByteArray(byte[] data) throws Exception {
		ByteBuffer buff = new ByteBuffer(data);
		header = buff.getStringArray();
		key = buff.getStringArray();

		int formatsize = buff.getInt();
		formats = new FormatType<?>[formatsize];
		for (int i = 0; i < formatsize; i++) {
			String type = buff.getString();
			formats[i] = FormatTypes.valueOf(type).getType();
		}
		byte[] secondAsBytes = buff.getByteArray();
		if (secondAsBytes.length != 0) {
			BlockFormats type = BlockFormats.valueOf(buff.getString());
			secondFormat = BlockFormat.getFormat(type, secondAsBytes);
		}
		return this;
	}

	public byte[] getData(String k, byte[] d) {
		int offset = 0;
		for (int i = 0; i < header.length; i++) {
			if (!header[i].equals(k)) {
				offset += formats[i].size(offset, d);
			} else {
				return formats[i].getData(offset, d);
			}

		}
		return null;
	}

	public FormatType<?> getFormat(int i) {
		return formats[i];
	}

	FormatType<?> getFormat(String el) {
		return formats[headerPos(el)];
	}

	private FormatType<?>[] getFormats(String[] key2) {
		FormatType<?>[] formats = new FormatType<?>[key2.length];
		for (int i = 0; i < key2.length; i++) {
			formats[i] = getFormat(key2[i]);
		}
		return formats;
	}

	@Override
	public byte[] getKey(BlockData data) {
		ByteBuffer buff = new ByteBuffer();
		for (String h : key) {
			byte[] val = getData(h, data.toByteArray());
			buff.putRawByteArray(val);
		}
		return buff.build();
	}

	@Override
	public BlockFormat getKeyFormat() {
		BlockFormat ret = new RecordFormat(key, getFormats(key), key);
		return ret;
	}

	private int getOffset(String k, byte[] data) {
		int offset = 0;
		for (int i = 0; i < header.length; i++) {
			if (!header[i].equals(k)) {
				offset += formats[i].size(offset, data);
			} else
				return offset;
		}
		return offset;

	}

	public int getPos(String k) {
		return headerPos(k);
	}

	@Override
	public BlockFormats getType() {
		return BlockFormats.RECORD;
	}

	private int headerPos(String el) {
		for (int i = 0; i < header.length; i++) {
			if (header[i].equals(el))
				return i;
		}
		return -1;
	}

	public Record newRecord() {
		return new Record(this);
	}

	@Override
	public String print(byte[] bs) {
		boolean first = true;
		StringBuilder builder = new StringBuilder();
		builder.append("(");
		for (int i = 0; i < header.length; i++) {
			if (first)
				first = false;
			else
				builder.append(",");
			String k = header[i];
			builder.append(k + ":" + formats[i].get(getData(k, bs)));
		}
		builder.append(")");
		return builder.toString();
	}

	public int size() {
		return header.length;
	}

	@Override
	public byte[] toByteArray() throws Exception {
		ByteBuffer buff = new ByteBuffer();
		buff.putStringArray(header);
		buff.putStringArray(key);
		buff.putInt(formats.length);
		for (FormatType<?> formatType : formats) {
			buff.putString(formatType.getType().name());
		}

		if (secondFormat != null) {
			buff.putByteArray(secondFormat.toByteArray());
			BlockFormats type = secondFormat.getType();
			buff.putString(type.name());
		} else
			buff.putByteArray(new byte[] {});
		return buff.build();
	}
}
