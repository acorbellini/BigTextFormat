package edu.bigtextformat.index;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import edu.bigtextformat.Range;
import edu.bigtextformat.block.Block;
import edu.bigtextformat.block.BlockFile;
import edu.bigtextformat.block.BlockFileOptions;
import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.block.BlockPosChangeListener;
import edu.bigtextformat.header.Header;
import edu.jlime.util.DataTypeUtils;
import edu.jlime.util.compression.CompressionType;

public class BplusIndex implements Index, Iterable<IndexData>,
		BlockPosChangeListener {

	private static long magic = DataTypeUtils.byteArrayToLong("BPLUSIDX"
			.getBytes());

	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

	private static int headerSize = 128;

	private static final int DEFAULT_INDEX_SIZE = 40;

	private static final int DEFAULT_INDEX_BLOCK_SIZE = 512;

	BlockFile file;

	private IndexData root;

	private int maxIndexSize = DEFAULT_INDEX_SIZE;

	private int maxLeafSize = DEFAULT_INDEX_SIZE * 2;

	private int indexBlockSize = DEFAULT_INDEX_BLOCK_SIZE;

	private Header h;

	private BlockFormat format;

	Cache<Long, IndexData> indexDatas = CacheBuilder.newBuilder().weakValues()
			.<Long, IndexData> build();

	private volatile long lastSplitted = 0;

	private boolean parallel = true;

	// Index deleted; -> Tal vez no sea necesario, si es que el indice no es muy
	// grande y se puede compactar. En ese caso, los marcados como DELETED se
	// puede guardar en memoria.

	public BplusIndex(String string, BlockFormat format, boolean trunc,
			boolean write) throws Exception {
		this.format = format;
		this.file = BlockFile.create(string,
				new BlockFileOptions().setMagic(magic).setTrunc(trunc)
						.setReadOnly(!write));
		this.file.addPosListener(this);
		this.h = file.getHeader();
		byte[] rootAddrAsBytes = h.get("root");

		if (rootAddrAsBytes != null) {
			long rootPos = DataTypeUtils.byteArrayToLong(rootAddrAsBytes);
			this.root = getIndexData(rootPos);
		} else if (!file.isEmpty()) {
			throw new Exception("Can't find root pointer in header.");
		} else {
			Block b = file.newEmptyBlock();
			createRoot(b, 0);

		}
	}

	@Override
	public void changedPosition(Block b, long oldPos) throws Exception {
		if (b.equals(root.getBlock()))
			h.putData("root", DataTypeUtils.longToByteArray(b.getPos()));
		IndexData data = getIndexData(b.getPos());
		data.updateBlockPosition();
	}

	@Override
	public void close() throws Exception {
		file.close();
	}

	private IndexData createIndexData(Block b, long parent, int level)
			throws Exception {
		return IndexData.create(this, b, parent, level, true,
				CompressionType.SNAPPY.getComp());
	}

	private void createRoot(Block b, int level) throws Exception {

		this.root = createIndexData(b, -1, level);
		this.root.persist();

		h.putData("root",
				DataTypeUtils.longToByteArray(root.getBlock().getPos()));

		synchronized (indexDatas) {
			indexDatas.put(b.getPos(), root);
		}
	}

	private IndexData findBlock(byte[] key) throws Exception {
		IndexData current = root;
		while (current.level() != 0) {
			Long son = DataTypeUtils.byteArrayToLong(current.getSon(key));
			current = getIndexData(son);
		}
		return current;
	}

	@Override
	public byte[] get(byte[] k) throws Exception {
		Lock read = lock.readLock();
		read.lock();
		try {
			return findBlock(k).get(k);
		} finally {
			read.unlock();
		}
	}

	@Override
	public long getBlockPosition(byte[] k) throws Exception {
		IndexData data = findBlock(k);
		return data.getBlock().getPos();
	}

	public IndexData getFirstData() throws Exception {
		IndexData cur = root;
		while (cur.getLevel() != 0) {
			cur = getIndexData(cur.getFirstSon());
		}
		return cur;
	}

	public BlockFormat getFormat() {
		return format;
	}

	IndexData getIndexData(byte[] addr) throws Exception {
		long pos = DataTypeUtils.byteArrayToLong(addr);
		return getIndexData(pos);
	}

	IndexData getIndexData(final long pos) throws Exception {
		if (root != null && root.getBlock().getPos() == pos)
			return root;

		// Block block = file.getBlock(pos, false);
		return indexDatas.get(pos, new Callable<IndexData>() {

			@Override
			public IndexData call() throws Exception {
				return IndexData.read(BplusIndex.this,
						file.getBlock(pos, false));

			}
		});
	}

	public int getIndexSize() {
		return maxIndexSize;
	}

	private void insert(IndexData data, byte[] k, byte[] value, byte[] vRight)
			throws Exception {
		data.put(k, value, vRight);
		if ((data.level == 0 && data.size() > maxLeafSize)
				|| ((data.level != 0) && (data.size() > maxIndexSize))) {
			lastSplitted++;
			IndexData parent = null;
			IndexData split = data.split();
			byte[] sendItUp = split.last();
			if (data.getParent() == -1) {
				Block b = file.newEmptyBlock();
				createRoot(b, data.level + 1);
				data.setParent(root.getBlock().getPos());
				split.setParent(root.getBlock().getPos());
				parent = root;
			} else {
				parent = getIndexData(data.getParent());
			}

			split.setNext(data.getBlock().getPos());
			split.setPrev(data.getPrev());
			split.persist(); // final persist

			indexDatas.put(split.getBlock().getPos(), split);

			IndexData prev = null;
			if (data.getPrev() != -1) {
				prev = getIndexData(data.getPrev());
				prev.setNext(split.getBlock().getPos());

			}
			List<IndexData> sons = new ArrayList<>();
			if (split.getLevel() != 0) {

				for (byte[] b : split.getValues()) {
					IndexData son = getIndexData(b);
					son.setParent(split.getBlock().getPos());
					sons.add(son);
				}

			}

			data.setPrev(split.getBlock().getPos());
			data.persist();// final persist

			if (prev != null)
				prev.persist();

			for (IndexData indexData : sons) {
				indexData.persist();
			}
			insert(parent, sendItUp,
					DataTypeUtils.longToByteArray(split.getBlock().getPos()),
					DataTypeUtils.longToByteArray(data.getBlock().getPos()));
		} else
			data.persist();
	}

	@Override
	public Iterator<IndexData> iterator() {
		return new BplusIndexIterator(this);
	}

	@Override
	public Iterator<Long> iterator(Range range) {
		return null;
	}

	public String print() throws Exception {
		StringBuilder builder = new StringBuilder();
		IndexData curr = root;

		do {
			IndexData son = null;
			if (curr.getLevel() != 0) {
				byte[] firstSon = curr.getFirstSon();
				if (firstSon != null) {
					son = getIndexData(firstSon);
				}
			}
			builder.append(curr.print());
			while (curr.getNext() != -1) {
				builder.append("<--->");
				curr = getIndexData(curr.getNext());
				builder.append(curr.print());
			}
			builder.append("\n");
			curr = son;
		} while (curr != null);
		return builder.toString();

	}

	@Override
	public void put(byte[] key, byte[] value, boolean ifNotPresent)
			throws Exception {
		if (parallel) {
			boolean done = false;
			while (!done) {
				long currSplitTime = lastSplitted;
				ReadLock read = lock.readLock();
				read.lock();

				IndexData data = null;
				try {
					data = findBlock(key);
					if (ifNotPresent && data.get(key) != null)
						return;
				} catch (Exception e) {
					throw e;
				} finally {
					read.unlock();
				}

				WriteLock write = lock.writeLock();
				write.lock();
				try {
					if (currSplitTime == lastSplitted) {
						insert(data, key, value, null);
						done = true;
					}
				} catch (Exception e) {
					throw e;
				} finally {
					write.unlock();
				}

			}
		} else {
			WriteLock write = lock.writeLock();
			write.lock();
			IndexData data = findBlock(key);
			if (ifNotPresent && data.get(key) != null)
				return;
			insert(data, key, value, null);
			write.unlock();
		}
	}

	@Override
	public void splitRange(Range orig, Range left, Range right, byte[] leftPos)
			throws Exception {
		// IndexData data = findBlock(right.getLast());
		// data.removeBelow(right.getFirst());
		// insert(left.getLast(), leftPos, null);
	}
}
