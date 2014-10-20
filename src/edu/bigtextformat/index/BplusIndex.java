package edu.bigtextformat.index;

import java.util.Iterator;

import edu.bigtextformat.Range;
import edu.bigtextformat.block.Block;
import edu.bigtextformat.block.BlockFile;
import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.header.Header;
import edu.jlime.util.DataTypeUtils;

public class BplusIndex implements Index {

	private static long magic = DataTypeUtils.byteArrayToLong(new byte[] { 0xf,
			0xa, 0xc, 0xa, 0x1, 0x2, 0x3, 0x4 });

	private static int headerSize = 128;

	private static final int DEFAULT_INDEX_SIZE = 3;

	private static final int DEFAULT_INDEX_BLOCK_SIZE = 128;

	BlockFile file;

	private IndexData root;

	private int maxIndexSize = DEFAULT_INDEX_SIZE;

	private int indexBlockSize = DEFAULT_INDEX_BLOCK_SIZE;

	private Header h;

	private BlockFormat format;

	public BplusIndex(String string, BlockFormat format) throws Exception {
		this.format = format;
		this.file = BlockFile.createOrRead(string, headerSize, magic);
		this.h = file.getHeader();

		byte[] root = h.get("root");

		if (root == null) {
			long rootPos = DataTypeUtils.byteArrayToLong(root);
			this.root = new IndexData(file, format, indexBlockSize,
					maxIndexSize, rootPos, -1, 0).fromByteArray(file.getBlock(
					rootPos).payload());
		} else {
			Block b = new Block(indexBlockSize);
			long pos = file.append(b);
			this.root = new IndexData(file, format, indexBlockSize,
					maxIndexSize, pos, -1, 0);

		}
	}

	// Index deleted; -> Tal vez no sea necesario, si es que el indice no es muy
	// grande y se puede compactar. En ese caso, los marcados como DELETED se
	// puede guardar en memoria.

	@Override
	public Iterator<Long> iterator(Range range) {
		return null;
	}

	@Override
	public long getBlockPosition(byte[] k) throws Exception {
		IndexData data = findBlock(k);
		return data.pos();
	}

	@Override
	public void put(byte[] key, long value) throws Exception {

		insert(key, value, -1);
	}

	private void insert(byte[] k, long vLeft, long vRight) throws Exception {
		IndexData data = findBlock(k);
		data.put(k, vLeft, vRight);

		if (data.size() > maxIndexSize) {
			IndexData split = data.split();

			byte[] sendItUp = split.last();

			// file.writeBlock(data.pos(),
			// new Block(indexBlockSize, data.toByteArray()));
			// file.append(new Block(indexBlockSize, split.toByteArray()));

			insert(sendItUp, split.pos(), data.pos());
		}
	}

	private IndexData findBlock(byte[] key) throws Exception {
		IndexData current = root;
		while (current.level() != 0) {
			Long son = current.getSon(key);
			if (son > 0) {
				Block block = file.getBlock(son);
				current = new IndexData(file, format, indexBlockSize,
						maxIndexSize, son, current.pos, current.level + 1)
						.fromByteArray(block.payload());
			} else {
				Block b = new Block(indexBlockSize);
				long pos = file.append(b);
				return new IndexData(file, format, indexBlockSize,
						maxIndexSize, pos, current.pos, current.level + 1);
			}
		}
		return current;
	}

	@Override
	public void splitRange(Range orig, Range left, Range right, long leftPos)
			throws Exception {
		IndexData data = findBlock(right.getLast());
		data.removeBelow(right.getFirst());
		insert(left.getLast(), leftPos, -1);
	}

}
