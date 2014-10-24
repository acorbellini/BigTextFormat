package edu.bigtextformat.block;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.WeakHashMap;

import edu.bigtextformat.header.Header;
import edu.bigtextformat.raw.RawFile;
import edu.jlime.util.DataTypeUtils;

public class BlockFile implements Closeable, Iterable<Block> {
	private static class BlockFileIterator implements Iterator<Block> {
		Block current = null;
		private BlockFile file;

		public BlockFileIterator(BlockFile file) throws Exception {
			this.file = file;
		}

		@Override
		public Block next() {
			return current;
		}

		@Override
		public boolean hasNext() {
			try {
				// do {
				if (current == null)
					current = file.getFirstBlock();
				else
					current = file.getBlock(current.getNextBlockPos(), false);
				// } while (current.isDeleted());
				return true;
			} catch (Exception e) {
				e.printStackTrace();
			}
			return false;
		}
	}

	long magic;
	RawFile file;
	private Header header;
	private int minSize;
	long currentPos = 0;
	boolean compressed = false;

	private static WeakHashMap<Long, Block> blocks = new WeakHashMap<>();

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
		Block res = blocks.get(pos);
		if (res == null) {
			synchronized (blocks) {
				res = blocks.get(pos);
				if (res == null) {
					if (pos > 0 && currentPos > pos) {
						res = Block.read(this, pos);
					} else if (create) {
						res = newBlock(new byte[] {}, size);
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
			long magic, boolean compressed) throws Exception {
		RawFile file = RawFile.getChannelRawFile(path, false);

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
		ret.header = Header.open(headerBlock);
		ret.minSize = minSize;
		return ret;
	}

	public Block newEmptyBlock() throws Exception {
		return newBlock(null, minSize);
	}

	public synchronized long reserve(int max) {
		long res = currentPos;
		currentPos += max;
		return res;
	}

	public Block newBlock(byte[] bytes) throws Exception {
		return newBlock(bytes, minSize);
	}

	private Block newBlock(byte[] bytes, int size) throws Exception {
		Block b = Block.create(this, size);
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
		return length() == 8l + header.size();
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
		return getBlock(8l + header.size(), false);
	}

	public void removeBlock(long pos) {
		synchronized (blocks) {
			blocks.remove(pos);
		}
	}

	public RawFile getFile() {
		return file;
	}
}
