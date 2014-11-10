package edu.bigtextformat.levels;

import java.util.Collections;
import java.util.List;

import edu.bigtextformat.block.Block;
import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.record.DataType;
import edu.jlime.util.ByteBuffer;

public class DataBlock implements DataType<DataBlock> {

	ByteArrayList keys = new ByteArrayList();
	ByteArrayList values = new ByteArrayList();
	private Block b;

	private int size = 0;

	public DataBlock() {
	}

	public synchronized void add(byte[] k, byte[] val) {
		keys.add(k);
		values.add(val);
		size += k.length + val.length + 4 + 4;
	}

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

	public List<byte[]> getKeys() {
		return keys;
	}

	public List<byte[]> getValues() {
		return values;
	}

	@Override
	public byte[] toByteArray() throws Exception {
		ByteBuffer buff = new ByteBuffer();
		buff.putByteArray(keys.getVals().toArray());
		buff.putIntArray(keys.getIndexes().toArray());
		buff.putByteArray(values.getVals().toArray());
		buff.putIntArray(values.getIndexes().toArray());
		return buff.build();
	}

	@Override
	public DataBlock fromByteArray(byte[] data) throws Exception {
		ByteBuffer buff = new ByteBuffer(data);
		keys = new ByteArrayList(buff.getByteArray(), buff.getIntArray());
		values = new ByteArrayList(buff.getByteArray(), buff.getIntArray());
		return this;
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

	public boolean contains(byte[] k, BlockFormat format) {
		int cont = Collections.binarySearch(keys, k, format);
		if (cont < 0)
			return false;
		return true;
	}

	public DataBlockIterator iterator() {
		return new DataBlockIterator(this);
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
