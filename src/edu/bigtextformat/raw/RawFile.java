package edu.bigtextformat.raw;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import edu.jlime.util.DataTypeUtils;

public abstract class RawFile implements Closeable {

	public static RawFile getChannelRawFile(String path, boolean trunc, boolean write)
			throws Exception {
		return new RawFileChannel(path, trunc, write);
	}

	public abstract long length() throws Exception;

	public long readLong(long i) throws Exception {
		byte[] l = readBytes(i, 8);
		return DataTypeUtils.byteArrayToLong(l);
	}

	public int readInt(long pos) throws Exception {
		byte[] l = readBytes(pos, 4);
		return DataTypeUtils.byteArrayToInt(l);
	}

	public byte[] readBytes(long i, int size) throws Exception {
		byte[] ret = new byte[size];
		read(i, ret, 0, size);
		return ret;
	}

	public abstract void write(long pos, byte[] byteArray) throws Exception;

	public abstract void writeByte(long l, byte b) throws Exception;

	public abstract void read(long pos, byte[] data, int offset, int size)
			throws Exception;

	public abstract void sync() throws IOException;

	public abstract void read(long pos, byte[] data) throws Exception;

	public abstract ByteBuffer memMap(long pos, long nextBlockPos)
			throws IOException;

	public abstract File getFile();

	public abstract void delete() throws IOException;

}
