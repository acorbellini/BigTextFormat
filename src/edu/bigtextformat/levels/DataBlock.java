package edu.bigtextformat.levels;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import edu.bigtextformat.block.Block;
import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.record.DataType;
import edu.jlime.util.ByteBuffer;

public class DataBlock implements DataType<DataBlock> {

	// ByteArrayList keys = new ByteArrayList();
	// ByteArrayList values = new ByteArrayList();

	byte[] k_list;
	int[] k_index;
	byte[] v_list;
	int[] v_index;

	private Block b;

	private int size = 0;

	public DataBlock() {
	}

	public DataBlock(byte[] k, int[] ki, byte[] v, int[] vi) {
		this.k_list = k;
		this.k_index = ki;
		this.v_list = v;
		this.v_index = vi;

	}

	// public synchronized void add(byte[] k, byte[] val) {
	// keys.add(k);
	// values.add(val);
	// size += k.length + val.length + 4 + 4;
	// }

	// public synchronized void insertOrdered(byte[] k, byte[] val,
	// BlockFormat format) {
	// // while (cont < keys.size() && format.compare(k, keys.get(cont)) >
	// // 0)
	// // cont++;
	// int cont = Collections.binarySearch(keys, k, format);
	// if (cont < 0)
	// cont = -(cont + 1);
	// keys.add(cont, k);
	// values.add(cont, val);
	// size += k.length + val.length;
	// }

	// public List<byte[]> getKeys() {
	// return keys;
	// }
	//
	// public List<byte[]> getValues() {
	// return values;
	// }

	@Override
	public byte[] toByteArray() throws Exception {
		ByteBuffer buff = new ByteBuffer();
		buff.putByteArray(k_list);
		buff.putIntArray(k_index);
		buff.putByteArray(v_list);
		buff.putIntArray(v_index);
		return buff.build();
	}

	@Override
	public DataBlock fromByteArray(byte[] data) throws Exception {
		ByteBuffer buff = new ByteBuffer(data);
		// keys = new ByteArrayList(buff.getByteArray(), buff.getIntArray());
		// values = new ByteArrayList(buff.getByteArray(), buff.getIntArray());
		k_list = buff.getByteArray();
		k_index = buff.getIntArray();
		v_list = buff.getByteArray();
		v_index = buff.getIntArray();
		return this;
	}

	public int size() {
		return size;
	}

	public byte[] lastKey() {
		return getKey(k_index.length - 1);
	}

	byte[] getKey(int i) {
		return Arrays.copyOfRange(k_list, k_index[i],
				i + 1 >= k_index.length ? k_list.length : k_index[i + 1]);
	}

	byte[] getValue(int i) {
		return Arrays.copyOfRange(v_list, v_index[i],
				i + 1 >= v_index.length ? v_list.length : v_index[i + 1]);
	}

	public byte[] firstKey() {
		return getKey(0);
	}

	public void setBlock(Block b) {
		this.b = b;
	}

	public Block getBlock() {
		return b;
	}

	public boolean contains(byte[] k, BlockFormat format) {
		int pos = search(k, format);
		if (pos < 0)
			return false;
		return true;

	}

	private int search(byte[] k, BlockFormat format) {
		boolean found = false;
		int lo = 0;
		int hi = k_index.length - 1;
		int cont = 0;
		while (lo <= hi && !found) {
			int mid = lo + (hi - lo) / 2;
			byte[] key = getKey(mid);
			int comp = format.compare(k, key);
			if (comp < 0) {
				hi = mid - 1;
				cont = lo;
			} else if (comp > 0) {
				lo = mid + 1;
				cont = lo;
			} else {
				found = true;
				cont = mid;
			}
		}
		if (!found)
			cont = -cont - 1;
		return cont;
	}

	public DataBlockIterator iterator() {
		return new DataBlockIterator(this);
	}

	public String print(BlockFormat format) {
		StringBuilder builder = new StringBuilder();
		for (int j = 0; j < k_index.length; j++) {
			builder.append(format.print(getKey(j)));
		}
		return builder.toString();
	}

	public int indexSize() {
		return k_index.length;
	}

	public Pair<byte[], byte[]> getFirstBetween(byte[] from, boolean inclFrom,
			byte[] to, boolean inclTo, BlockFormat format) {
		int pos = search(from, format);
		if (pos < 0) {
			pos = -(pos + 1);
		} else if (!inclFrom)
			pos = pos + 1;
		if (pos >= k_index.length)
			return null;

		byte[] cs = getKey(pos);
		int compare = format.compare(cs, to);
		if (compare > 0)
			return null;

		if (compare == 0 && !inclTo)
			return null;

		return Pair.create(cs, getValue(pos));
	}
}
