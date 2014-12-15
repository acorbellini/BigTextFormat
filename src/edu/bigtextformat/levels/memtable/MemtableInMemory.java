package edu.bigtextformat.levels.memtable;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import edu.bigtextformat.block.Block;
import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.record.FormatType;
import edu.bigtextformat.record.FormatTypes;
import edu.bigtextformat.record.RecordFormat;
import edu.bigtextformat.util.Pair;
import edu.jlime.util.DataTypeUtils;

public class MemtableInMemory {

	public static void main(String[] args) {

		List<Integer> toAdd = Arrays.asList(new Integer[] { 4, 5, 6, 1, 3, 0,
				2, 14, 56, 23 });

		BlockFormat format = RecordFormat.create(new String[] { "nada" },
				new FormatType[] { FormatTypes.INTEGER.getType() },
				new String[] { "nada" });
		MemtableInMemory table = new MemtableInMemory(format);
		for (Integer integer : toAdd) {
			System.out.println(table.print());
			System.err.println("insert " + integer);
			table.insertOrdered(DataTypeUtils.intToByteArray(integer),
					new byte[] {});
		}
		System.out.println(table.print());
		// for (byte[] integer : table.keys) {
		// System.out.println(DataTypeUtils.byteArrayToInt(integer));
		// }
	}
	TreeMap<byte[], byte[]> data;

	private Block b;
	private int size = 0;

	private BlockFormat format;

	public MemtableInMemory(final BlockFormat format) {
		this.format = format;
		data = new TreeMap<byte[], byte[]>(new Comparator<byte[]>() {

			@Override
			public int compare(byte[] arg0, byte[] arg1) {
				return format.compare(arg0, arg1);
			}
		});
	}

	public void clear() {
		data.clear();
		size = 0;
	}

	public boolean contains(byte[] k) {
		return data.containsKey(k);
	}

	public byte[] firstKey() {
		return data.firstKey();
	}

	public byte[] get(byte[] k) {
		return data.get(k);
	}

	public Block getBlock() {
		return b;
	}

	public Pair<byte[], byte[]> getFirstIntersect(byte[] from,
			boolean inclFrom, byte[] to, boolean inclTo, BlockFormat format) {
		// int cont = Search.search(from, keys, format);
		// if (cont < 0)
		// cont = -(cont + 1);
		// else if (!inclFrom)
		// cont = cont + 1;
		//
		// if (cont >= keys.size())
		// return null;
		//
		// byte[] cs = keys.get(cont);
		// int compare = format.compare(cs, to);
		// if (compare > 0)
		// return null;
		//
		// if (compare == 0 && !inclTo)
		// return null;
		//
		// return Pair.create(cs, values.get(cont));

		SortedMap<byte[], byte[]> sm = data.subMap(from, to);
		if (sm.isEmpty())
			return null;
		return Pair.create(sm.firstKey(), sm.get(sm.firstKey()));
	}

	public synchronized void insertOrdered(byte[] k, byte[] val) {
		data.put(k, val);
		size += k.length + val.length + 4 + 4;
	}

	public byte[] lastKey() {
		return data.lastKey();
	}

	public String print() {
		StringBuilder builder = new StringBuilder();
		Set<byte[]> es = data.keySet();
		for (byte[] k : es) {
			builder.append(format.print(k));
		}
		return builder.toString();
	}

	public void setBlock(Block b) {
		this.b = b;
	}

	public int size() {
		return size;
	}
}
