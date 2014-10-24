package edu.bigtextformat.index;

import java.util.Iterator;

import edu.bigtextformat.Range;
import edu.bigtextformat.block.Block;
import edu.bigtextformat.block.BlockFile;
import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.header.Header;
import edu.jlime.util.DataTypeUtils;

public class BplusIndex implements Index, Iterable<IndexData> {

	private static long magic = DataTypeUtils.byteArrayToLong("BPLUSIDX"
			.getBytes());

	private static int headerSize = 128;

	private static final int DEFAULT_INDEX_SIZE = 4;

	private static final int DEFAULT_INDEX_BLOCK_SIZE = 128;

	BlockFile file;

	private IndexData root;

	private int maxIndexSize = DEFAULT_INDEX_SIZE;

	private int indexBlockSize = DEFAULT_INDEX_BLOCK_SIZE;

	private Header h;

	private BlockFormat format;

	public BplusIndex(String string, BlockFormat format) throws Exception {
		this.format = format;
		this.file = BlockFile.open(string, headerSize, indexBlockSize, magic,
				true);
		this.h = file.getHeader();
		byte[] rootAddrAsBytes = h.get("root");

		if (rootAddrAsBytes != null) {
			long rootPos = DataTypeUtils.byteArrayToLong(rootAddrAsBytes);
			this.root = IndexData.read(this, file.getBlock(rootPos, false));
		} else if (!file.isEmpty()) {
			throw new Exception("Can't find root pointer in header.");
		} else {
			Block b = file.newEmptyBlock();
			this.root = createRoot(b);
			this.root.persist();
			h.putData("root", DataTypeUtils.longToByteArray(b.getPos()));
		}
	}

	private IndexData createRoot(Block b) throws Exception {
		return createIndexData(b, -1, 0);
		// long rootPos = this.root.getBlock().getPos();
		// IndexData left = createIndexData(file.newEmptyBlock(), rootPos, 0);
		// IndexData right = createIndexData(file.newEmptyBlock(), rootPos, 0);
		// left.setNext(right.getBlock().getPos());
		// right.setPrev(left.getBlock().getPos());
	}

	private IndexData createIndexData(Block b, long parent, int level)
			throws Exception {
		return IndexData.create(this, b, parent, level);
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
		return data.getBlock().getPos();
	}

	@Override
	public void put(byte[] key, byte[] value) throws Exception {
		IndexData data = findBlock(key);
		insert(data, key, value, null);
	}

	private void insert(IndexData data, byte[] k, byte[] value, byte[] vRight)
			throws Exception {

		data.put(k, value, vRight);

		if (data.size() > maxIndexSize) {
			IndexData split = data.split();

			byte[] sendItUp = split.last();
			IndexData parent;
			if (data.parent == -1) {
				this.root = IndexData.create(this, file.newEmptyBlock(), -1,
						data.level + 1);
				root.persist();
				h.putData("root",
						DataTypeUtils.longToByteArray(root.getBlock().getPos()));

				data.parent = root.getBlock().getPos();
				split.parent = root.getBlock().getPos();

				parent = root;
			} else {
				parent = getIndexData(data.parent);
			}

			split.persist(); // need a block pos
			
			split.setNext(data.getNext());			
			split.setPrev(data.getBlock().getPos());
			
			data.setNext(split.getBlock().getPos());
			

			split.persist(); // final persist
			data.persist();// final persist

			insert(parent, sendItUp,
					DataTypeUtils.longToByteArray(split.getBlock().getPos()),
					DataTypeUtils.longToByteArray(data.getBlock().getPos()));
		}
		data.persist();
	}

	private IndexData getIndexData(long pos) throws Exception {
		return IndexData.read(this, file.getBlock(pos, false));
	}

	private IndexData findBlock(byte[] key) throws Exception {
		IndexData current = root;
		while (current.level() != 0) {
			Long son = DataTypeUtils.byteArrayToLong(current.getSon(key));
			Block block = file.getBlock(son, false);
			current = IndexData.read(this, block);
		}
		return current;
	}

	@Override
	public void splitRange(Range orig, Range left, Range right, byte[] leftPos)
			throws Exception {
		// IndexData data = findBlock(right.getLast());
		// data.removeBelow(right.getFirst());
		// insert(left.getLast(), leftPos, null);
	}

	@Override
	public byte[] get(byte[] k) throws Exception {
		return findBlock(k).get(k);
	}

	public int getIndexSize() {
		return maxIndexSize;
	}

	public BlockFormat getFormat() {
		return format;
	}

	@Override
	public Iterator<IndexData> iterator() {
		return new BplusIndexIterator(this);
	}

	public IndexData getFirstData() throws Exception {
		IndexData cur = root;
		while (cur.level != 0) {
			long pos = DataTypeUtils.byteArrayToLong(root.getFirstSon());
			cur = getIndexData(pos);
		}
		return cur;
	}

}
