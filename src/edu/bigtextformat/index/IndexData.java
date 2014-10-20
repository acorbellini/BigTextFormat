package edu.bigtextformat.index;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.bigtextformat.block.Block;
import edu.bigtextformat.block.BlockFile;
import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.record.DataType;
import edu.jlime.util.ByteBuffer;

public class IndexData implements DataType<IndexData> {
	int level;

	long pos;

	long parent;

	long nextSibling;

	long prevSibling;

	BlockFormat k;

	private List<byte[]> keys;

	private List<Long> values;

	BlockFile file;

	private int indexSize;

	private int blockSize;

	public IndexData(BlockFile file, BlockFormat k, int blockSize,
			int indexSize, long pos, long parent, int level) {
		this.blockSize = blockSize;
		this.file = file;
		this.k = k;
		this.pos = pos;
		this.parent = parent;
		this.indexSize = indexSize;
		this.keys = new ArrayList<byte[]>(indexSize);
		this.values = new ArrayList<Long>(indexSize + 1); // keys.size+1
		this.level = level;
	}

	public int level() {
		return level;
	}

	public Long getSon(byte[] key) {
		if (keys.isEmpty())
			return -1l;

		for (int i = 0; i < keys.size(); i++) {
			if (k.compare(key, keys.get(i)) <= 0)
				return values.get(i);
		}
		return values.get(values.size() - 1);
	}

	public void put(byte[] key, long vLeft, long vRight) {
		for (int i = 0; i < keys.size(); i++) {
			if (k.compare(key, keys.get(i)) <= 0) {
				keys.add(i, key);
				values.set(i, vLeft);
				values.set(i + 1, vRight);
				return;
			}
		}
		keys.add(keys.size(), key);
		values.add(values.size(), vLeft);
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
		buff.putLongList(values);
		return buff.build();
	}

	@Override
	public IndexData fromByteArray(byte[] data) throws Exception {
		ByteBuffer buff = new ByteBuffer(data);
		level = buff.getInt();
		keys = buff.getByteArrayList();
		values = buff.getLongList();
		return this;
	}

	public IndexData split() throws Exception {
		Block b = new Block(blockSize);
		long pos = file.append(b);
		IndexData ret = new IndexData(file, k, blockSize, indexSize, pos,
				parent, level);		
		int i = 0;
		Iterator<Long> valueit = values.iterator();
		for (Iterator<byte[]> iterator = keys.iterator(); iterator.hasNext()
				&& i < keys.size() / 2; i++) {
			byte[] bs = (byte[]) iterator.next();
			Long left = valueit.next();
			iterator.remove();
			valueit.remove();
			ret.put(bs, left, -1);
		}
		return ret;
	}

	public byte[] last() {
		return keys.get(keys.size() - 1);
	}

	public long parent() {
		return parent;
	}

	public long pos() {
		return pos;
	}

	public void removeBelow(byte[] first) {
		Iterator<Long> valueit = values.iterator();
		for (Iterator<byte[]> iterator = keys.iterator(); iterator.hasNext();) {
			byte[] bs = (byte[]) iterator.next();
			valueit.next();
			if (k.compare(first, bs) > 0) {
				iterator.remove();
				valueit.remove();
			}
		}
	}
}
