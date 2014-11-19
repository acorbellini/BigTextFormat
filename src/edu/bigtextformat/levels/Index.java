package edu.bigtextformat.levels;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.record.DataType;
import edu.bigtextformat.util.Search;
import edu.jlime.util.ByteBuffer;

public class Index implements DataType<Index> {
	List<byte[]> keys = new ArrayList<>();
	List<Long> blocks = new ArrayList<>();
	byte[] minKey = null;
	byte[] maxKey = null;
	private BlockFormat format;

	public Index(BlockFormat format) {
		this.format = format;
	}

	public void setMaxKey(byte[] maxKey) {
		this.maxKey = maxKey;
	}

	public void setMinKey(byte[] minKey) {
		this.minKey = minKey;
	}

	public byte[] getMinKey() {
		return minKey;
	}

	public byte[] getMaxKey() {
		return maxKey;
	}

	public void put(byte[] k, long blockPos, BlockFormat format) {
		int pos = Search.search(k, keys, format);
		if (pos < 0) {
			pos = -(pos + 1);
		}
		// else{
		// if(Arrays.equals(keys.get(pos), k))
		// System.out.println("Not ok");
		// }

		keys.add(pos, k);
		blocks.add(pos, blockPos);
	}

	public byte[] toByteArray() {
		ByteBuffer buff = new ByteBuffer();
		buff.putByteArrayList(keys);
		buff.putLongList(blocks);
		buff.putByteArray(minKey);
		buff.putByteArray(maxKey);
		return buff.build();
	}

	@Override
	public Index fromByteArray(byte[] data) throws Exception {
		ByteBuffer buff = new ByteBuffer(data);
		keys = buff.getByteArrayList();
		blocks = buff.getLongList();
		minKey = buff.getByteArray();
		maxKey = buff.getByteArray();
		return this;
	}

	public long get(byte[] k) {
		if (minKey != null && maxKey != null) {
			if (format.compare(k, minKey) < 0 || format.compare(k, maxKey) > 0)
				return -1;
		}

		int cont = Search.search(k, keys, format);
		if (cont < 0) {
			cont = -(cont + 1);
		}
		if (cont == keys.size())
			return -1;
		else
			return blocks.get(cont);
	}

	public Iterator<Long> range(byte[] from, byte[] to, BlockFormat format) {
		if (format.compare(to, minKey) < 0 || format.compare(from, maxKey) > 0)
			return new IndexRangeIterator(1, 0, this);
		int fromPos = Search.search(from, keys, format);
		if (fromPos < 0)
			fromPos = -(fromPos + 1);

		int toPos = Search.search(to, keys, format);
		if (toPos < 0)
			toPos = -(toPos + 1);

		return new IndexRangeIterator(fromPos, toPos, this);
	}

	public int size() {
		return keys.size();
	}

	public List<byte[]> getKeys() {
		return keys;
	}

	public List<Long> getBlocks() {
		return blocks;
	}
}