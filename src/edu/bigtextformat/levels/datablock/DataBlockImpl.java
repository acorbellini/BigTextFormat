package edu.bigtextformat.levels.datablock;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.TreeMap;

import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.levels.levelfile.LevelFile;
import edu.bigtextformat.record.DataType;
import edu.bigtextformat.util.Pair;
import edu.jlime.util.ByteBuffer;

public class DataBlockImpl implements DataType<DataBlockImpl>, DataBlock {

	private static final int MAX_SEARCH = 200;

	// ByteArrayList keys = new ByteArrayList();
	// ByteArrayList values = new ByteArrayList();

	TreeMap<byte[], Integer> cache;

	private LevelFile levelFile;

	byte[] k_list;
	int[] k_index;
	byte[] v_list;
	int[] v_index;

	// private Block b;
	Long blockPos;

	private long size = 0;
	private Long blockSize;

	private BlockFormat format;

	public DataBlockImpl(byte[] k, int[] ki, byte[] v, int[] vi) {
		this.k_list = k;
		this.k_index = ki;
		this.v_list = v;
		this.v_index = vi;
		this.size = k.length + 4 + ki.length + 4 + v.length + 4 + vi.length + 4;
	}

	public DataBlockImpl(LevelFile levelFile2, long pos, long len,
			BlockFormat format) {
		this.levelFile = levelFile2;
		this.blockPos = pos;
		this.blockSize = len;
		this.format = format;
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.bigtextformat.levels.DataBlock#contains(byte[],
	 * edu.bigtextformat.block.BlockFormat)
	 */
	@Override
	public boolean contains(byte[] k) {
		int pos = search(k);
		if (pos < 0)
			return false;
		return true;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.bigtextformat.levels.DataBlock#firstKey()
	 */
	@Override
	public byte[] firstKey() {
		return getKey(0);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.bigtextformat.levels.DataBlock#fromByteArray(byte[])
	 */
	@Override
	public DataBlockImpl fromByteArray(byte[] data) throws Exception {
		ByteBuffer buff = new ByteBuffer(data);
		// keys = new ByteArrayList(buff.getByteArray(), buff.getIntArray());
		// values = new ByteArrayList(buff.getByteArray(), buff.getIntArray());
		k_list = buff.getByteArray();
		k_index = buff.getIntArray();
		v_list = buff.getByteArray();
		v_index = buff.getIntArray();

		return this;
	}

	@Override
	public byte[] get(byte[] k) {
		int pos = search(k);
		if (pos < 0)
			return null;
		return getValue(pos);
	}

	@Override
	public Long getBlockPos() {
		return blockPos;
	}

	@Override
	public LevelFile getFile() {
		return levelFile;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.bigtextformat.levels.DataBlock#getFirstBetween(byte[], boolean,
	 * byte[], boolean, edu.bigtextformat.block.BlockFormat)
	 */
	@Override
	public Pair<byte[], byte[]> getFirstBetween(byte[] from, boolean inclFrom,
			byte[] to, boolean inclTo) {
		int pos = search(from);
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

	public byte[] getKey(int i) {
		return Arrays.copyOfRange(k_list, k_index[i],
				i + 1 >= k_index.length ? k_list.length : k_index[i + 1]);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.bigtextformat.levels.DataBlock#getBlock()
	 */
	// @Override
	// public Block getBlock() {
	// return b;
	// }

	@Override
	public Long getLen() {
		return blockSize;
	}

	public byte[] getValue(int i) {
		return Arrays.copyOfRange(v_list, v_index[i],
				i + 1 >= v_index.length ? v_list.length : v_index[i + 1]);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.bigtextformat.levels.DataBlock#indexSize()
	 */
	@Override
	public int indexSize() {
		return k_index.length;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.bigtextformat.levels.DataBlock#iterator()
	 */
	@Override
	public DataBlockIterator iterator() {
		return new DataBlockIterator(this);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.bigtextformat.levels.DataBlock#lastKey()
	 */
	@Override
	public byte[] lastKey() {
		return getKey(k_index.length - 1);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.bigtextformat.levels.DataBlock#print(edu.bigtextformat.block.BlockFormat
	 * )
	 */
	@Override
	public String print(BlockFormat format) {
		StringBuilder builder = new StringBuilder();
		for (int j = 0; j < k_index.length; j++) {
			builder.append(format.print(getKey(j)));
		}
		return builder.toString();
	}

	private int search(byte[] k) {
		int max = 0;
		int min = 0;
		TreeMap<byte[], Integer> cache = getCache();

		if (cache.isEmpty()) {
			max = k_index.length - 1;
			min = 0;
		} else {
			Entry<byte[], Integer> ceilingEntry = cache.ceilingEntry(k);
			if (ceilingEntry != null) {
				max = ceilingEntry.getValue();
				min = max - MAX_SEARCH;
			} else {
				max = k_index.length - 1;
				Entry<byte[], Integer> lastEntry = cache.lastEntry();
				if(lastEntry==null)
					System.out.println("what?");
				min = lastEntry.getValue() + 1;
			}
		}
		boolean found = false;
		int lo = min;
		int hi = max;
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

	private TreeMap<byte[], Integer> getCache() {
		if (cache == null) {
			synchronized (this) {
				if (cache == null) {
					this.cache = new TreeMap<>(new Comparator<byte[]>() {
						@Override
						public int compare(byte[] arg0, byte[] arg1) {
							return DataBlockImpl.this.format
									.compare(arg0, arg1);
						}
					});
					int cacheSize = (k_index.length - 1) / MAX_SEARCH;
					for (int i = 1; i <= cacheSize; i++) {
						cache.put(getKey(i * MAX_SEARCH), i * MAX_SEARCH);
					}
				}
			}
		}
		return cache;
	}

	public void setBlockPos(Long pos) {
		this.blockPos = pos;
	}

	public void setBlockSize(Long len) {
		this.blockSize = len;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.bigtextformat.levels.DataBlock#size()
	 */
	@Override
	public long size() {
		return size;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.bigtextformat.levels.DataBlock#toByteArray()
	 */
	@Override
	public byte[] toByteArray() throws Exception {
		ByteBuffer buff = new ByteBuffer(k_list.length + 4 + k_index.length * 4
				+ 4 + v_list.length + 4 + v_index.length * 4 + 4);
		buff.putByteArray(k_list);
		buff.putIntArray(k_index);
		buff.putByteArray(v_list);
		buff.putIntArray(v_index);
		return buff.build();
	}
}
