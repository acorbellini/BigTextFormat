package edu.bigtextformat.record;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.data.BlockData;
import edu.jlime.util.ByteBuffer;

public class RecordFormat extends BlockFormat implements DataType<RecordFormat> {
	List<String> header;
	List<FormatType<?>> formats;
	private List<String> key;

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
		for (String k : header) {
			int c = compare(k, d1, d2);
			if (c != 0)
				return c;
		}
		return 0;
	}

	private int compare(String k, byte[] d1, byte[] d2) {
		int pos = header.indexOf(k);

		byte[] k1 = getData(k, d1);
		byte[] k2 = getData(k, d2);

		FormatType<?> f = formats.get(pos);
		return f.compare(k1, k2);
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
			buff.putString(formatType.toString());
		}
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
}
