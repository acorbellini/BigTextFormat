package edu.bigtextformat.block;

import java.io.IOException;
import java.util.Iterator;

import edu.bigtextformat.header.Header;
import edu.bigtextformat.raw.RawFile;
import edu.jlime.util.DataTypeUtils;

public class BlockFile {
	RawFile file;
	private Header header;

	private BlockFile(RawFile file2, Header h) throws IOException {
		this.file = file2;
		this.header = h;
	}

	public long append(Block b) throws Exception {
		long writePos = file.length();
		writeBlock0(writePos, b);
		return writePos;
	}

	private void writeBlock0(long pos, Block b) throws Exception {
		file.write(pos, b.toByteArray());
	}

	public long writeBlock(long pos, Block b) throws Exception {
		long writePos = pos;
		byte[] byteArray = b.toByteArray();

		int currentBlockSize = file.readInt(pos);
		if (currentBlockSize <= byteArray.length) {
			setBlockStatus(pos + 4 + 4, (byte) (b.getStatus() & 0x1));
			writePos = file.length();
		}
		writeBlock0(writePos, b);
		return writePos;
	}

	public Block getBlock(long pos) throws Exception {
		int blockSize = file.readInt(pos);
		byte[] data = new byte[blockSize + 4];
		DataTypeUtils.intToByteArray(blockSize, data);
		file.read(pos + 4, data, 4, data.length - 4);
		return new Block(blockSize).fromByteArray(data);
	}

	public void setBlockStatus(long pos, byte status) throws Exception {
		file.writeByte(pos + 4 + 4, status);
	}

	public Iterator<Block> iterate(final Iterator<Long> pos) {
		return new Iterator<Block>() {

			@Override
			public Block next() {
				try {
					return getBlock(pos.next());
				} catch (Exception e) {
					e.printStackTrace();
				}
				return null;
			}

			@Override
			public boolean hasNext() {
				return pos.hasNext();
			}
		};
	}

	public Header getHeader() {
		return header;
	}

	public static BlockFile createOrRead(String path, int headerSize, long magic)
			throws Exception {
		RawFile file = RawFile.getChannelRawFile(path, false);
		Header h = Header.readOrCreateHeader(file, headerSize, magic);
		return new BlockFile(file, h);
	}

}
