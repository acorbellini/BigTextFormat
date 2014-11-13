package edu.bigtextformat.levels;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import edu.bigtextformat.block.Block;
import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.record.FormatType;
import edu.bigtextformat.record.FormatTypes;
import edu.bigtextformat.record.RecordFormat;
import edu.bigtextformat.util.Search;
import edu.jlime.util.DataTypeUtils;

public class Memtable {

	List<byte[]> keys = new ArrayList<>();
	List<byte[]> values = new ArrayList<>();
	private Block b;

	private int size = 0;

	public Memtable() {
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
		// int cont = Collections.binarySearch(keys, k, format);
		int cont = Search.search(k, keys, format);

		if (cont < 0)
			cont = -(cont + 1);
		keys.add(cont, k);
		values.add(cont, val);
		size += k.length + val.length + 4 + 4;
	}

	public List<byte[]> getKeys() {
		return keys;
	}

	public List<byte[]> getValues() {
		return values;
	}

	// public byte[] toByteArray() throws Exception {
	// ByteArrayList keys = new ByteArrayList();
	// for (byte[] bs : this.keys) {
	// keys.add(bs);
	// }
	//
	// ByteArrayList values = new ByteArrayList();
	// for (byte[] bs : this.values) {
	// values.add(bs);
	// }
	// ByteBuffer buff = new ByteBuffer();
	// buff.putByteArray(keys.getVals().toArray());
	// buff.putIntArray(keys.getIndexes().toArray());
	// buff.putByteArray(values.getVals().toArray());
	// buff.putIntArray(values.getIndexes().toArray());
	// return buff.build();
	// }

	// @Override
	// public DataBlock fromByteArray(byte[] data) throws Exception {
	// ByteBuffer buff = new ByteBuffer(data);
	// DataBlock mem = new DataBlock();
	// mem.keys = buff.getByteArrayList();
	// mem.values = buff.getByteArrayList();
	// return mem;
	// }

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

	// public void merge(DataBlock db, BlockFormat format) {
	// for (int i = 0; i < db.keys.size(); i++) {
	// insertOrdered(db.keys.get(i), db.values.get(i), format);
	// }
	// }

	public boolean contains(byte[] k, BlockFormat format) {
		int cont = Collections.binarySearch(keys, k, format);
		if (cont < 0)
			return false;
		return true;
	}

	// @Override
	// public Iterator<Pair<byte[], byte[]>> iterator() {
	// return new Iterator<Pair<byte[], byte[]>>() {
	// int i = 0;
	//
	// @Override
	// public Pair<byte[], byte[]> next() {
	// if (!hasNext())
	// return null;
	// Pair<byte[], byte[]> pair = new Pair<>(keys.get(i),
	// values.get(i));
	// i++;
	// return pair;
	// }
	//
	// @Override
	// public boolean hasNext() {
	// return i < keys.size();
	// }
	//
	// @Override
	// public void remove() {
	// // TODO Auto-generated method stub
	//
	// }
	// };
	// }

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

	public static void main(String[] args) {
		Memtable table = new Memtable();
		List<Integer> toAdd = Arrays.asList(new Integer[] { 4, 5, 6, 1, 3, 0,
				2, 14, 56, 23 });

		BlockFormat format = RecordFormat.create(new String[] { "nada" },
				new FormatType[] { FormatTypes.INTEGER.getType() },
				new String[] { "nada" });

		for (Integer integer : toAdd) {
			System.out.println(table.print(format));
			System.err.println("insert " + integer);
			table.insertOrdered(DataTypeUtils.intToByteArray(integer),
					new byte[] {}, format);
		}
		System.out.println(table.print(format));
		// for (byte[] integer : table.keys) {
		// System.out.println(DataTypeUtils.byteArrayToInt(integer));
		// }
	}

	public Pair<byte[], byte[]> getFirstIntersect(byte[] from,
			boolean inclFrom, byte[] to, boolean inclTo, BlockFormat format) {
		int cont = Search.search(from, keys, format);
		if (cont < 0)
			cont = -(cont + 1);
		else if (!inclFrom)
			cont = cont + 1;

		if (cont >= keys.size())
			return null;

		byte[] cs = keys.get(cont);
		int compare = format.compare(cs, to);
		if (compare > 0)
			return null;

		if (compare == 0 && !inclTo)
			return null;

		return Pair.create(cs, values.get(cont));
	}
}
