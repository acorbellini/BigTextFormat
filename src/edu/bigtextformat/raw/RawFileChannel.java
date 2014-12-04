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

public class RawFileChannel extends RawFile {
	// private RandomAccessFile f;
	private FileChannel f;
	private Path p;

	public RawFileChannel(String path, boolean trunc, boolean readOnly,
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
			opts.add(StandardOpenOption.SYNC);

		this.f = FileChannel.open(p, opts);
		// String mode = "r";
		// if (write)
		// mode = "rw";
		// this.f = new RandomAccessFile(new File(path), mode);
	}

	@Override
	public long length() throws FileChannelException {
		try {
			return f.size();
		} catch (Exception e) {
			throw new FileChannelException(this, e);
		}
		// return f.length();
	}

	@Override
	public void write(long pos, byte[] byteArray) throws Exception {
		ByteBuffer order = ByteBuffer.wrap(byteArray);
		f.write(order, pos);
		// f.setLength(pos + byteArray.length + 1);
		// f.write(byteArray, (int) pos, byteArray.length);
	}

	public void write(long pos, edu.jlime.util.ByteBuffer byteArray)
			throws Exception {
		ByteBuffer order = ByteBuffer.wrap(byteArray.getBuffered(),
				byteArray.getOffset(), byteArray.getWritePos());
		f.write(order, pos);
		// f.setLength(pos + byteArray.length + 1);
		// f.write(byteArray, (int) pos, byteArray.length);
	}

	@Override
	public void writeByte(long l, byte b) throws Exception {
		f.write(ByteBuffer.wrap(new byte[] { b }), l);
		// write(l, new byte[] { b });
	}

	@Override
	public void read(long pos, byte[] data, int offset, int size)
			throws Exception {
		ByteBuffer buff = ByteBuffer.wrap(data, offset, size);
		f.read(buff, pos);
		// f.read(data, offset, size);
	}

	@Override
	public void close() throws IOException {
		// System.out.println("Closed file " + getPath());
		if (!f.isOpen())
			return;
		sync();
		f.close();

	}

	@Override
	public void sync() throws IOException {
		f.force(false);
	}

	@Override
	public void read(long pos, byte[] data) throws Exception {
		read(pos, data, 0, data.length);
	}

	@Override
	public MappedByteBuffer memMap(long pos, long nextBlockPos)
			throws IOException {
		return f.map(MapMode.READ_WRITE, pos, nextBlockPos - pos);
	}

	@Override
	public File getFile() {
		return p.toFile();
	}

	@Override
	public void delete() throws IOException {
		close();
		Files.delete(p);
	}

	@Override
	public synchronized void copy(RawFile orig, long from, long len, long pos)
			throws IOException {
		FileChannel fc = ((RawFileChannel) orig).f;
		f.position(pos);
		try {
			fc.transferTo(from, len, f);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String getPath() {
		return p.toString();
	}
}
