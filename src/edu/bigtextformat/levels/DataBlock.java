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

	byte[] k;
	int[] ki;
	byte[] v;
	int[] vi;

	private Block b;

	private int size = 0;

	public DataBlock() {
	}

	public DataBlock(byte[] k, int[] ki, byte[] v, int[] vi) {
		this.k = k;
		this.ki = ki;
		this.v = v;
		this.vi = vi;

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
		buff.putByteArray(k);
		buff.putIntArray(ki);
		buff.putByteArray(v);
		buff.putIntArray(vi);
		return buff.build();
	}

	@Override
	public DataBlock fromByteArray(byte[] data) throws Exception {
		ByteBuffer buff = new ByteBuffer(data);
		// keys = new ByteArrayList(buff.getByteArray(), buff.getIntArray());
		// values = new ByteArrayList(buff.getByteArray(), buff.getIntArray());
		k = buff.getByteArray();
		ki = buff.getIntArray();
		v = buff.getByteArray();
		vi = buff.getIntArray();
		return this;
	}

	public int size() {
		return size;
	}

	public byte[] lastKey() {
		return get(ki.length - 1);
	}

	byte[] get(int i) {
		return Arrays.copyOfRange(k, ki[i], i + 1 >= ki.length ? k.length
				: ki[i + 1]);
	}

	public byte[] firstKey() {
		return get(0);
	}

	public void setBlock(Block b) {
		this.b = b;
	}

	public Block getBlock() {
		return b;
	}

	public boolean contains(byte[] k, BlockFormat format) {
		// int cont = Collections.binarySearch(new KeyReader(this), k, format);
		// if (cont < 0)
		// return false;
		// return true;

		// for (int i = 0; i < ki.length; i++) {
		// int compare = format.compare(k, get(i));
		// if (compare == 0)
		// return true;
		// if (compare < 0)
		// return false;
		// }
		// return false;

		int lo = 0;
		int hi = ki.length - 1;
		while (lo <= hi) {
			// Key is in a[lo..hi] or not present.
			int mid = lo + (hi - lo) / 2;
			int comp = format.compare(k, get(mid));
			if (comp < 0)
				hi = mid - 1;
			else if (comp > 0)
				lo = mid + 1;
			else
				return true;
			// get(mid);
		}
		return false;

	}

	public DataBlockIterator iterator() {
		return new DataBlockIterator(this);
	}

	public String print(BlockFormat format) {
		StringBuilder builder = new StringBuilder();
		// List<byte[]> keys = getKeys();
		for (int j = 0; j < ki.length; j++) {
			builder.append(format.print(get(j)));
		}
		return builder.toString();
	}

	// public void clear() {
	// keys.clear();
	// values.clear();
	// size = 0;
	// }

	public int indexSize() {
		return ki.length;
	}
}
