package edu.bigtextformat.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import edu.bigtextformat.block.Block;
import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.record.DataType;
import edu.jlime.util.ByteBuffer;

public class IndexData implements DataType<IndexData> {
	int level;

	long parent;

	private Block block;

	private long next = -1;

	private long prev = -1;

	private List<byte[]> keys;

	private List<byte[]> values;

	private BplusIndex index;

	private IndexData(BplusIndex bplusIndex) {
		this.index = bplusIndex;
	}

	private IndexData(BplusIndex bplusIndex, Block block, long parent, int level) {
		this.block = block;
		this.parent = parent;
		this.index = bplusIndex;
		this.keys = new ArrayList<byte[]>(index.getIndexSize());
		this.values = new ArrayList<byte[]>(index.getIndexSize() + 1); // keys.size+1
		this.level = level;
	}

	public int level() {
		return level;
	}

	public byte[] getSon(byte[] key) {
		if (keys.isEmpty())
			return null;
		BlockFormat k = index.getFormat();
		for (int i = 0; i < keys.size(); i++) {
			if (k.compare(key, keys.get(i)) <= 0)
				return values.get(i);
		}
		return values.get(values.size() - 1);
	}

	public void put(byte[] key, byte[] vLeft, byte[] vRight) throws Exception {
		BlockFormat k = index.getFormat();
		for (int i = 0; i < keys.size(); i++) {
			int compare = k.compare(key, keys.get(i));
			if (compare == 0) {
				values.set(i, vLeft);
				if (vRight != null)
					values.set(i + 1, vRight);
				return;
			} else if (compare < 0) {
				keys.add(i, key);
				values.add(i, vLeft);
				if (vRight != null)
					values.set(i + 1, vRight); // Siempre tengo uno de más en
												// values porque lo agrego al
												// principio
				return;
			}
		}
		keys.add(keys.size(), key);
		values.add(values.size(), vLeft);
		if (vRight != null)
			values.add(values.size(), vRight);
	}

	public int size() {
		return keys.size();
	}

	@Override
	public byte[] toByteArray() {
		ByteBuffer buff = new ByteBuffer();
		buff.putInt(level);
		buff.putByteArrayList(keys);
		buff.putByteArrayList(values);
		return buff.build();
	}

	@Override
	public IndexData fromByteArray(byte[] data) throws Exception {
		ByteBuffer buff = new ByteBuffer(data);
		level = buff.getInt();
		keys = buff.getByteArrayList();
		values = buff.getByteArrayList();
		return this;
	}

	public IndexData split() throws Exception {
		IndexData newData = IndexData.create(index, index.file.newEmptyBlock(),
				parent, level);
		int mid = keys.size() / 2;
		for (int i = 0; i < mid; i++) {
			newData.put(keys.get(i), values.get(i), null);
		}
		for (int i = mid; i < keys.size(); i++) {
			keys.remove(0);
			values.remove(0);
		}
		return newData;
	}

	public byte[] last() {
		return keys.get(keys.size() - 1);
	}

	public long parent() {
		return parent;
	}

	public void removeBelow(byte[] first) {
		BlockFormat k = index.getFormat();
		Iterator<byte[]> valueit = values.iterator();
		for (Iterator<byte[]> iterator = keys.iterator(); iterator.hasNext();) {
			byte[] bs = iterator.next();
			valueit.next();
			if (k.compare(first, bs) > 0) {
				iterator.remove();
				valueit.remove();
			}
		}
	}

	public byte[] get(byte[] k) {
		for (int i = 0; i < keys.size(); i++) {
			if (Arrays.equals(keys.get(i), k)) {
				return values.get(i);
			}
		}
		return null;
	}

	public static IndexData read(BplusIndex bplusIndex, Block block)
			throws Exception {
		IndexData data = new IndexData(bplusIndex, block, -1, -1);
		data.fromByteArray(block.payload());
		return data;
	}

	public Block getBlock() {
		return block;
	}

	public void setNext(long right) {
		this.next = right;
	}

	public void setPrev(long left) {
		this.prev = left;
	}

	public static IndexData create(BplusIndex bplusIndex, Block b, long parent,
			int level) throws Exception {
		return new IndexData(bplusIndex, b, parent, level);
	}

	public void persist() throws Exception {
		this.block.setPayload(toByteArray());

	}

	public long getNext() {
		return next;
	}

	public long getPrev() {
		return prev;
	}

	public byte[] getFirstSon() {
		return values.get(0);
	}
	
	public List<byte[]> getValues() {
		return values;
	}
}
