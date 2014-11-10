package edu.bigtextformat.block;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import edu.bigtextformat.header.Header;
import edu.bigtextformat.raw.RawFile;
import edu.jlime.util.DataTypeUtils;

public class BlockFile implements Closeable, Iterable<Block> {

	boolean reuseDeleted;

	TreeMap<Integer, List<Long>> deleted = new TreeMap<>();

	private List<BlockPosChangeListener> blockPosChangeListeners = new ArrayList<>();

	long magic;
	RawFile file;
	private Header header;
	private int minSize;
	long currentPos = 0;
	boolean compressed = false;

	// private static WeakHashMap<Block, Boolean> current = new WeakHashMap<>();
	//
	// private static WeakHashMap<Long, Block> blocks = new WeakHashMap<>();
	Cache<Long, Block> blocks = CacheBuilder.newBuilder().softValues()
			.<Long, Block> build();

	private BlockFile(RawFile file) {
		this.file = file;
	}

	public void setCompressed(boolean compressed) {
		this.compressed = compressed;
	}

	void writeBlock(long pos, Block block, byte[] bytes) throws Exception {
		file.write(pos, bytes);
		synchronized (blocks) {
			blocks.put(pos, block);
		}

		if (header != null && file.length() != header.getFsize())
			header.setFileSize(file.length());
	}

	public Block getBlock(long pos, boolean create) throws Exception {
		return getBlock(pos, minSize, create);
	}

	private Block getBlock(long pos, int size, boolean create) throws Exception {
		Block res = blocks.getIfPresent(pos);
		if (res == null) {
			synchronized (blocks) {
				res = blocks.getIfPresent(pos);
				if (res == null) {
					if (pos > 0 && currentPos > pos) {
						res = Block.read(this, pos);
					} else if (create) {
						res = newBlock(new byte[] {}, size, false);
					} else
						throw new Exception("Block does not exist");
					blocks.put(pos, res);
				}
			}
		}
		return res;
	}

	public Header getHeader() {
		return header;
	}

	public static BlockFile open(String path, int headerSize, int minSize,
			long magic, boolean compressed, boolean trunc, boolean write)
			throws Exception {
		RawFile file = RawFile.getChannelRawFile(path, trunc, write);

		BlockFile ret = new BlockFile(file);
		ret.setCompressed(compressed);
		ret.currentPos = file.length();
		if (file.length() > 0l) {
			// exists
			ret.magic = file.readLong(0);
			if (ret.magic != magic)
				throw new Exception("Wrong File Type");
		} else {
			ret.reserve(8);
			file.write(0, DataTypeUtils.longToByteArray(magic));
		}
		Block headerBlock = ret.getBlock(8l, headerSize, true);
		headerBlock.setFixed(true);
		// headerBlock.setMemoryMapped(true);
		ret.header = Header.open(headerBlock);
		ret.minSize = minSize;
		return ret;
	}

	public Block newEmptyBlock() throws Exception {
		return newBlock(null, minSize, false);
	}

	public long reserve(int max) {
		synchronized (blocks) {
			Entry<Integer, List<Long>> e = deleted.ceilingEntry(max);
			if (e == null || e.getValue().isEmpty()) {
				long res = currentPos;
				currentPos += max;
				return res;
			} else {
				return e.getValue().remove(0);
			}
		}
	}

	public Block newBlock(byte[] bytes) throws Exception {
		return newBlock(bytes, minSize, false);
	}

	private Block newBlock(byte[] bytes, int size, boolean fixed)
			throws Exception {
		Block b = Block.create(this, size);
		b.setFixed(fixed);
		b.setCompressed(compressed);
		if (bytes != null)
			b.setPayload(bytes);
		return b;
	}

	public void close() throws IOException {
		file.close();
	}

	public long length() throws Exception {
		return file.length();
	}

	public boolean isEmpty() throws Exception {
		return length() == getFirstBlockPos();
	}

	@Override
	public Iterator<Block> iterator() {
		try {
			return new BlockFileIterator(this);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	protected Block getFirstBlock() throws Exception {
		return getBlock(getFirstBlockPos(), false);
	}

	public long getFirstBlockPos() {
		return 8l + header.size();
	}

	public void removeBlock(long pos, int size, byte status) throws Exception {
		synchronized (blocks) {
			Block.setDeleted(file, status, pos);
			blocks.invalidate(pos);
			List<Long> l = deleted.get(size);
			if (l == null) {
				l = new ArrayList<>();
				deleted.put(size, l);
			}
			l.add(pos);
		}
	}

	public RawFile getRawFile() {
		return file;
	}

	public void addPosListener(BlockPosChangeListener l) {
		blockPosChangeListeners.add(l);
	}

	void notifyPosChanged(Block b, long oldPos) throws Exception {
		for (BlockPosChangeListener block : blockPosChangeListeners) {
			block.changedPosition(b, oldPos);
		}
	}

	public void removePosListener(BlockPosChangeListener l) {
		blockPosChangeListeners.remove(l);
	}

	public long size() throws Exception {
		return file.length();
	}

	public void delete() throws IOException {
		file.delete();
	}

	public Block newFixedBlock(byte[] byteArray) throws Exception {
		return newBlock(byteArray, minSize, true);
	}

}
