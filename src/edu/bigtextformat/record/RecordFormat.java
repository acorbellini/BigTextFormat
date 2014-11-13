package edu.bigtextformat.record;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.block.BlockFormats;
import edu.bigtextformat.data.BlockData;
import edu.jlime.util.ByteBuffer;

public class RecordFormat extends BlockFormat {
	List<String> header;
	List<FormatType<?>> formats;
	private List<String> key;
	private BlockFormat secondFormat;

	public RecordFormat(List<String> h, List<FormatType<?>> f, List<String> k) {
		this.header = h;
		this.formats = f;
		this.key = k;
	}

	public RecordFormat() {
	}

	FormatType<?> getFormat(String el) {
		return formats.get(header.indexOf(el));
	}

	@Override
	public byte[] getKey(BlockData data) {
		Record r = (Record) data;
		ByteBuffer buff = new ByteBuffer();
		for (String h : key) {
			byte[] val = getData(h, data.toByteArray());
			buff.putRawByteArray(val);
		}
		return buff.build();
	}

	@Override
	public int compare(byte[] d1, byte[] d2) {
		for (int i = 0; i < header.size(); i++) {
			String k = header.get(i);
			int c = compare(k, d1, d2);
			if (c != 0)
				return c;
		}
		if (secondFormat != null)
			return secondFormat.compare(d1, d2);
		return 0;
	}

	private int compare(String k, byte[] d1, byte[] d2) {
		int pos = header.indexOf(k);

		FormatType<?> f = formats.get(pos);
		return f.compare(d1, getOffset(k, d1), d2, getOffset(k, d2));
	}

	private int getOffset(String k, byte[] data) {
		int offset = 0;
		for (int i = 0; i < header.size(); i++) {
			if (!header.get(i).equals(k)) {
				offset += formats.get(i).size(offset, data);
			} else
				return offset;
		}
		return offset;

	}

	private byte[] getData(String k, byte[] d) {
		int offset = 0;
		for (int i = 0; i < header.size(); i++) {
			if (!header.get(i).equals(k)) {
				offset += formats.get(i).size(offset, d);
			} else {
				return formats.get(i).getData(offset, d);
			}

		}
		return null;
	}

	@Override
	public byte[] toByteArray() throws Exception {
		ByteBuffer buff = new ByteBuffer();
		buff.putStringList(header);
		buff.putStringList(key);
		buff.putInt(formats.size());
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

	@Override
	public RecordFormat fromByteArray(byte[] data) throws Exception {
		ByteBuffer buff = new ByteBuffer(data);
		header = buff.getStringList();
		key = buff.getStringList();
		formats = new ArrayList<>();
		int formatsize = buff.getInt();
		for (int i = 0; i < formatsize; i++) {
			String type = buff.getString();
			formats.add(FormatTypes.valueOf(type).getType());
		}
		byte[] secondAsBytes = buff.getByteArray();
		if (secondAsBytes.length != 0) {
			BlockFormats type = BlockFormats.valueOf(buff.getString());
			secondFormat = BlockFormat.getFormat(type, secondAsBytes);
		}
		return this;
	}

	@Override
	public BlockFormat getKeyFormat() {
		BlockFormat ret = new RecordFormat(key, getFormats(key), key);
		return ret;
	}

	private List<FormatType<?>> getFormats(List<String> key2) {
		List<FormatType<?>> formats = new ArrayList<>();
		for (String k : key2) {
			formats.add(getFormat(k));
		}
		return formats;
	}

	public static RecordFormat create(String[] types,
			FormatType<?>[] formatTypes, String[] k) {
		return new RecordFormat(Arrays.asList(types),
				Arrays.asList(formatTypes), Arrays.asList(k));
	}

	public Record newRecord() {
		return new Record(this);
	}

	public int size() {
		return header.size();
	}

	public int getPos(String k) {
		return header.indexOf(k);
	}

	public FormatType<?> getFormat(int i) {
		return formats.get(i);
	}

	@Override
	public String print(byte[] bs) {
		boolean first = true;
		StringBuilder builder = new StringBuilder();
		builder.append("(");
		for (int i = 0; i < header.size(); i++) {
			if (first)
				first = false;
			else
				builder.append(",");
			String k = header.get(i);
			builder.append(k + ":" + formats.get(i).get(getData(k, bs)));
		}
		builder.append(")");
		return builder.toString();
	}

	public void addKey(BlockFormat keyFormat) {
		secondFormat = keyFormat;
	}

	@Override
	public BlockFormats getType() {
		return BlockFormats.RECORD;
	}
}
