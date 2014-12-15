package edu.bigtextformat.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import edu.bigtextformat.block.Block;
import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.record.DataType;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.DataTypeUtils;
import edu.jlime.util.compression.CompressionType;
import edu.jlime.util.compression.Compressor;

public class IndexData implements DataType<IndexData> {
	public static IndexData create(BplusIndex bplusIndex, Block b, long parent,
			int level, boolean compressed, Compressor comp) throws Exception {
		return new IndexData(bplusIndex, b, parent, level, compressed, comp);
	}

	public static IndexData read(BplusIndex bplusIndex, Block block)
			throws Exception {
		IndexData data = new IndexData(bplusIndex, block, -1, -1, false, null);
		data.fromByteArray(block.payload());
		return data;
	}

	int level;

	long parent;

	private Block block;

	private long next = -1;

	private long prev = -1;

	private List<byte[]> keys;

	private List<byte[]> values;

	private BplusIndex index;

	private boolean binarySearch = true;

	private boolean compressed = true;

	private Compressor comp;

	private IndexData(BplusIndex bplusIndex, Block block, long parent,
			int level, boolean compressed, Compressor comp) {
		this.block = block;
		this.parent = parent;
		this.index = bplusIndex;
		this.keys = new ArrayList<byte[]>(index.getIndexSize());
		this.values = new ArrayList<byte[]>(index.getIndexSize() + 1); // keys.size+1
		this.level = level;
		this.compressed = compressed;
		this.comp = comp;
	}

	@Override
	public IndexData fromByteArray(byte[] data) throws Exception {
		ByteBuffer buff = new ByteBuffer(data);
		this.level = buff.getInt();
		this.parent = buff.getLong();
		this.prev = buff.getLong();
		this.next = buff.getLong();
		this.compressed = buff.getBoolean();
		if (compressed)
			this.comp = CompressionType.getByID(buff.get());
		byte[] listsAsBytes = buff.getByteArray();
		if (this.compressed)
			listsAsBytes = comp.uncompress(listsAsBytes);
		ByteBuffer lists = new ByteBuffer(listsAsBytes);
		this.keys = lists.getByteArrayList();
		this.values = lists.getByteArrayList();
		return this;
	}

	public byte[] get(byte[] k) {
		for (int i = 0; i < keys.size(); i++) {
			if (Arrays.equals(keys.get(i), k)) {
				return values.get(i);
			}
		}
		return null;
	}

	public Block getBlock() {
		return block;
	}

	public byte[] getFirstSon() {
		if (values.isEmpty())
			return null;
		return values.get(0);
	}

	public List<byte[]> getKeys() {
		return keys;
	}

	public int getLevel() {
		return level;
	}

	public long getNext() {
		return next;
	}

	public long getParent() {
		return parent;
	}

	public long getPrev() {
		return prev;
	}

	public byte[] getSon(byte[] key) {
		if (binarySearch) {
			final BlockFormat format = index.getFormat();
			int pos = Collections.binarySearch(keys, key,
					new Comparator<byte[]>() {

						@Override
						public int compare(byte[] arg0, byte[] arg1) {
							return format.compare(arg0, arg1);
						}
					});
			if (pos < 0)
				pos = -(pos + 1);
			return values.get(pos);
		} else {
			if (keys.isEmpty())
				return null;
			BlockFormat k = index.getFormat();
			for (int i = 0; i < keys.size(); i++) {
				if (k.compare(key, keys.get(i)) <= 0)
					return values.get(i);
			}
			return values.get(values.size() - 1);
		}
	}

	public List<byte[]> getValues() {
		return values;
	}

	public byte[] last() {
		if (keys.isEmpty())
			return null;
		return keys.get(keys.size() - 1);
	}

	public int level() {
		return level;
	}

	public long parent() {
		return parent;
	}

	public void persist() throws Exception {
		this.block.setPayload(toByteArray());

	}

	public String print() {
		StringBuilder builder = new StringBuilder();
		builder.append("[");
		for (int i = 0; i < keys.size(); i++) {
			builder.append(index.getFormat().print(keys.get(i)));
		}
		builder.append("]");
		return builder.toString();
	}

	public String printSons() throws Exception {
		if (level == 0)
			return "";
		StringBuilder b = new StringBuilder();
		for (byte[] v : values) {
			b.append(index.getIndexData(v).print() + "--");
		}
		return b.toString();
	}

	public void put(byte[] key, byte[] vLeft, byte[] vRight) throws Exception {
		if (binarySearch) {
			final BlockFormat format = index.getFormat();
			if (vRight == null) { // leaf nodes
				int pos = Collections.binarySearch(keys, key,
						new Comparator<byte[]>() {

							@Override
							public int compare(byte[] arg0, byte[] arg1) {
								return format.compare(arg0, arg1);
							}
						});
				if (pos >= 0)
					values.set(pos, vLeft);
				else {
					pos = -(pos + 1);
					keys.add(pos, key);
					values.add(pos, vLeft);
				}
			} else {
				int pos = Collections.binarySearch(keys, key,
						new Comparator<byte[]>() {

							@Override
							public int compare(byte[] arg0, byte[] arg1) {
								return format.compare(arg0, arg1);
							}
						});
				if (pos >= 0) {
					values.add(pos, vLeft);
					// creo q no es necesario.
					// values.set(pos + 1, vRight);
				} else {
					pos = -(pos + 1);
					keys.add(pos, key);
					values.add(pos, vLeft);
					if (pos + 1 == values.size())
						values.add(pos + 1, vRight);
					// else
					// values.set(pos + 1, vRight);
				}
			}
		} else {
			BlockFormat k = index.getFormat();
			for (int i = 0; i < keys.size(); i++) {
				int compare = k.compare(key, keys.get(i));
				if (compare == 0) {
					values.set(i, vLeft);
					if (vRight != null) {
						values.set(i + 1, vRight);
					}
					return;
				} else if (compare < 0) {
					keys.add(i, key);
					values.add(i, vLeft);
					if (vRight != null)
						values.set(i + 1, vRight); // Siempre tengo uno de más
					// en
					// values porque lo agrego
					// al
					// principio
					return;
				}
			}

			if (values.size() == keys.size() + 1)
				values.set(values.size() - 1, vLeft);
			else
				values.add(vLeft);
			keys.add(key);
			if (vRight != null)
				values.add(values.size(), vRight);
		}

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

	public void setNext(long right) {
		this.next = right;
	}

	public void setParent(long parent) {
		this.parent = parent;
	}

	public void setPrev(long left) {
		this.prev = left;
	}

	public int size() {
		return keys.size();
	}

	public IndexData split() throws Exception {
		IndexData newData = IndexData.create(index, index.file.newEmptyBlock(),
				parent, level, compressed, comp);
		int mid = keys.size() / 2;
		for (int i = 0; i < mid; i++) {
			newData.put(keys.get(i), values.get(i), null);
		}
		for (int i = 0; i < mid; i++) {
			keys.remove(0);
			values.remove(0);
		}
		return newData;
	}

	@Override
	public byte[] toByteArray() {
		ByteBuffer buff = new ByteBuffer();
		buff.putInt(level);
		buff.putLong(parent);
		buff.putLong(prev);
		buff.putLong(next);
		buff.putBoolean(compressed);
		if (compressed)
			buff.put(comp.getType().getId());

		ByteBuffer lists = new ByteBuffer();
		lists.putByteArrayList(keys);
		lists.putByteArrayList(values);
		
		byte[] listsAsBytes = lists.build();
		if (this.compressed)
			listsAsBytes = comp.compress(lists.build());
		buff.putByteArray(listsAsBytes);
		return buff.build();
	}

	private void update(byte[] key, byte[] newVal) {
		if (keys.isEmpty())
			return; // might happen if parent is not created
		BlockFormat k = index.getFormat();
		for (int i = 0; i < keys.size(); i++) {
			int compare = k.compare(key, keys.get(i));
			if (compare == 0) {
				values.set(i, newVal);
				return;
			}
		}
		values.set(values.size() - 1, newVal);
	}

	public void updateBlockPosition() throws Exception {
		if (this.getPrev() != -1) {
			IndexData dataPrev = index.getIndexData(this.prev);
			dataPrev.setNext(block.getPos());
			dataPrev.persist();
		}
		if (this.getNext() != -1) {
			IndexData dataNext = index.getIndexData(this.getNext());
			dataNext.setPrev(block.getPos());
			dataNext.persist();
		}

		if (this.getParent() != -1) {
			IndexData dataParent = index.getIndexData(this.parent);
			dataParent.update(this.last(),
					DataTypeUtils.longToByteArray(block.getPos()));
			dataParent.persist();
		}
		if (getLevel() != 0) {
			for (byte[] son : values) {
				IndexData dataSon = index.getIndexData(son);
				dataSon.setParent(block.getPos());
				dataSon.persist();
			}
		}

	}
}
