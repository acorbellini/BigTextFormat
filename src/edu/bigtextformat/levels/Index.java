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

	public Index() {
	}

	public void put(byte[] k, long blockPos, BlockFormat format) {
		int pos = Search.search(k, keys, format);
		if (pos < 0) {
			pos = -(pos + 1);
		}
		keys.add(pos, k);
		blocks.add(pos, blockPos);
	}

	public byte[] toByteArray() {
		ByteBuffer buff = new ByteBuffer();
		buff.putByteArrayList(keys);
		buff.putLongList(blocks);
		return buff.build();
	}

	@Override
	public Index fromByteArray(byte[] data) throws Exception {
		ByteBuffer buff = new ByteBuffer(data);
		Index i = new Index();
		i.keys = buff.getByteArrayList();
		i.blocks = buff.getLongList();
		return i;
	}

	public long get(byte[] k, BlockFormat format) {
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
		int fromPos = Search.search(from, keys, format);
		if (fromPos < 0)
			fromPos = -(fromPos + 1);

		int toPos = Search.search(to, keys, format);
		if (toPos < 0)
			toPos = -(toPos + 1);
		
		
		return new IndexRangeIterator(fromPos, toPos, this	);
	}
}