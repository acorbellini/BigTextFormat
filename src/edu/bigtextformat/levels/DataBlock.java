package edu.bigtextformat.levels;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import edu.bigtextformat.block.Block;
import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.record.DataType;
import edu.jlime.util.ByteBuffer;

public class DataBlock implements DataType<DataBlock>,
		Iterable<Pair<byte[], byte[]>> {

	List<byte[]> keys = new ArrayList<>();
	List<byte[]> values = new ArrayList<>();
	private Block b;

	private int size = 0;

	public DataBlock() {
		this.keys = new ArrayList<>(512);
		this.values = new ArrayList<>(512);
	}

	public synchronized void add(byte[] k, byte[] val) {
		keys.add(k);
		values.add(val);
		size += k.length + val.length + 4 + 4;
	}

	public synchronized void insertOrdered(byte[] k, byte[] val,
			BlockFormat format) {
		// while (cont < keys.size() && format.compare(k, keys.get(cont)) >
		// 0)
		// cont++;
		int cont = Collections.binarySearch(keys, k, format);
		if (cont < 0)
			cont = -(cont + 1);
		keys.add(cont, k);
		values.add(cont, val);
		size += k.length + val.length;
	}

	public List<byte[]> getKeys() {
		return keys;
	}

	public List<byte[]> getValues() {
		return values;
	}

	@Override
	public byte[] toByteArray() throws Exception {
		ByteBuffer buff = new ByteBuffer(size + 4 + 4);
		buff.putByteArrayList(keys);
		buff.putByteArrayList(values);
		return buff.build();
	}

	@Override
	public DataBlock fromByteArray(byte[] data) throws Exception {
		ByteBuffer buff = new ByteBuffer(data);
		DataBlock mem = new DataBlock();
		mem.keys = buff.getByteArrayList();
		mem.values = buff.getByteArrayList();
		return mem;
	}

	public int size() {
		return size;
	}

	public byte[] lastKey() {
		return keys.get(keys.size() - 1);
	}

	public byte[] firstKey() {
		return keys.get(0);
	}

	public void setBlock(Block b) {
		this.b = b;
	}

	public Block getBlock() {
		return b;
	}

	public void merge(DataBlock db, BlockFormat format) {
		for (int i = 0; i < db.keys.size(); i++) {
			insertOrdered(db.keys.get(i), db.values.get(i), format);
		}
	}

	public boolean contains(byte[] k, BlockFormat format) {
		int cont = Collections.binarySearch(keys, k, format);
		if (cont < 0)
			return false;
		return true;
	}

	@Override
	public Iterator<Pair<byte[], byte[]>> iterator() {
		return new Iterator<Pair<byte[], byte[]>>() {
			int i = 0;

			@Override
			public Pair<byte[], byte[]> next() {
				if (!hasNext())
					return null;
				Pair<byte[], byte[]> pair = new Pair<>(keys.get(i),
						values.get(i));
				i++;
				return pair;
			}

			@Override
			public boolean hasNext() {
				return i < keys.size();
			}

			@Override
			public void remove() {
				// TODO Auto-generated method stub

			}
		};
	}

	public String print(BlockFormat format) {
		StringBuilder builder = new StringBuilder();
		List<byte[]> keys = getKeys();
		for (int j = 0; j < keys.size(); j++) {
			builder.append(format.print(keys.get(j)));
		}
		return builder.toString();
	}

	public void clear() {
		keys.clear();
		values.clear();
		size = 0;
	}
}
