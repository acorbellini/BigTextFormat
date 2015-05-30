package edu.bigtextformat.raw;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;

import edu.jlime.util.DataTypeUtils;

public class RawFile {
	private FileChannel f;
	private Path p;
	private int currentPos = 0;

	public RawFile(String path, boolean trunc, boolean readOnly,
			boolean appendOnly, boolean sync) throws IOException {
		this.p = Paths.get(path);

		Set<OpenOption> opts = new HashSet<>();
		if (appendOnly) {
			opts.add(StandardOpenOption.CREATE);
			opts.add(StandardOpenOption.WRITE);
			opts.add(StandardOpenOption.APPEND);
		} else {
			opts.add(StandardOpenOption.READ);
			if (!readOnly) {
				opts.add(StandardOpenOption.CREATE);
				opts.add(StandardOpenOption.WRITE);
			}
		}
		if (trunc)
			opts.add(StandardOpenOption.TRUNCATE_EXISTING);
		if (sync)
			opts.add(StandardOpenOption.DSYNC);

		this.f = FileChannel.open(p, opts);
	}

	public void close() throws IOException {
		if (!f.isOpen())
			return;
		sync();
		f.close();

	}

	public synchronized void copy(RawFile orig, long from, long len, long pos)
			throws IOException {
		FileChannel fc = orig.f;
		f.position(pos);
		try {
			fc.transferTo(from, len, f);
		} catch (Exception e) {
			System.out.println("Error transfering from " + orig.getPath()
					+ " to " + p);
			e.printStackTrace();
		}
	}

	public void delete() throws IOException {
		close();
		Files.delete(p);
	}

	public File getFile() {
		return p.toFile();
	}

	public String getPath() {
		return p.toString();
	}

	public long length() throws RawFileException {
		try {
			return f.size();
		} catch (Exception e) {
			throw new RawFileException(this, e);
		}
	}

	public MappedByteBuffer memMap(long pos, long nextBlockPos)
			throws IOException {
		return f.map(MapMode.READ_WRITE, pos, nextBlockPos - pos);
	}

	public void read(long pos, byte[] data) throws Exception {
		read(pos, data, 0, data.length);
	}

	public void read(long pos, byte[] data, int offset, int size)
			throws Exception {
		ByteBuffer buff = ByteBuffer.wrap(data, offset, size);
		f.read(buff, pos);
	}

	public synchronized void sync() throws IOException {
		f.force(false);
	}

	public void write(long pos, byte[] byteArray) throws Exception {
		ByteBuffer order = ByteBuffer.wrap(byteArray);
		f.write(order, pos);
	}

	public void write(long pos, edu.jlime.util.ByteBuffer byteArray)
			throws Exception {
		ByteBuffer order = ByteBuffer.wrap(byteArray.getBuffered(),
				byteArray.getOffset(), byteArray.getWritePos());
		f.write(order, pos);
	}

	public void writeByte(long l, byte b) throws Exception {
		f.write(ByteBuffer.wrap(new byte[] { b }), l);
	}

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

	public void write(byte[] ba) throws Exception {
		write(currentPos, ba);
		currentPos += ba.length;
	}

	public synchronized void append(byte[][] toWrite) throws IOException {
		ByteBuffer[] buffs = new ByteBuffer[toWrite.length];
		for (int i = 0; i < toWrite.length; i++)
			buffs[i] = ByteBuffer.wrap(toWrite[i]);
		f.write(buffs);
	}
}
