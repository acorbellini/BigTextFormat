package edu.bigtextformat.raw;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import edu.jlime.util.DataTypeUtils;

public abstract class RawFile implements Closeable {

	public static RawFile getChannelRawFile(String path, boolean trunc,
			boolean readOnly, boolean appendOnly, boolean sync)
			throws Exception {
		return new RawFileChannel(path, trunc, readOnly, appendOnly, sync);
	}

	public abstract void copy(RawFile orig, long from, long len, long pos)
			throws IOException;

	public abstract void delete() throws IOException;

	public abstract File getFile();

	public abstract String getPath();

	public abstract long length() throws Exception;

	public abstract ByteBuffer memMap(long pos, long nextBlockPos)
			throws IOException;

	public abstract void read(long pos, byte[] data) throws Exception;

	public abstract void read(long pos, byte[] data, int offset, int size)
			throws Exception;

	public byte[] readBytes(long i, int size) throws Exception {
		byte[] ret = new byte[size];
		read(i, ret, 0, size);
		return ret;
	}

	public int readInt(long pos) throws Exception {
		byte[] l = readBytes(pos, 4);
		return DataTypeUtils.byteArrayToInt(l);
	}

	public long readLong(long i) throws Exception {
		byte[] l = readBytes(i, 8);
		return DataTypeUtils.byteArrayToLong(l);
	}

	public abstract void sync() throws IOException;

	public abstract void write(long pos, byte[] byteArray) throws Exception;

	public abstract void writeByte(long l, byte b) throws Exception;
}
