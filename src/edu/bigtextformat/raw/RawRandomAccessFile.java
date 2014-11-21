package edu.bigtextformat.raw;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class RawRandomAccessFile extends RawFile {
	private RandomAccessFile f;
	// private FileChannel f;
	private Path p;

	public RawRandomAccessFile(String path, boolean trunc, boolean write)
			throws IOException {
		this.p = Paths.get(path);
		//
		// List<OpenOption> opts = new ArrayList<>();
		// opts.add(StandardOpenOption.READ);
		// if (write)
		// opts.add(StandardOpenOption.WRITE);
		// if (trunc)
		// opts.add(StandardOpenOption.TRUNCATE_EXISTING);
		// opts.add(StandardOpenOption.CREATE);
		//
		// this.f = FileChannel.open(p, opts.toArray(new OpenOption[] {}));
		String mode = "r";
		if (write)
			mode = "rw";
		this.f = new RandomAccessFile(new File(path), mode);
	}

	@Override
	public synchronized long length() throws Exception {
		// return f.size();
		return f.length();
	}

	@Override
	public synchronized void write(long pos, byte[] byteArray) throws Exception {
		// ByteBuffer order = ByteBuffer.wrap(byteArray);
		// f.write(order, pos);
		f.seek(pos);
		f.write(byteArray, 0, byteArray.length);
	}

	@Override
	public synchronized void writeByte(long l, byte b) throws Exception {
		// f.write(ByteBuffer.wrap(new byte[] { b }), l);
		write(l, new byte[] { b });
	}

	@Override
	public synchronized void read(long pos, byte[] data, int offset, int size)
			throws Exception {
		// ByteBuffer buff = ByteBuffer.wrap(data, offset, size);
		// f.read(buff, pos);
		f.seek(pos);
		f.read(data, offset, size);
	}

	@Override
	public synchronized void close() throws IOException {
		sync();
		f.close();
		// System.out.println("Closed file " + p);
	}

	@Override
	public synchronized void sync() throws IOException {
		// f.force(false);
	}

	@Override
	public synchronized void read(long pos, byte[] data) throws Exception {
		read(pos, data, 0, data.length);
	}

	@Override
	public synchronized MappedByteBuffer memMap(long pos, long nextBlockPos)
			throws IOException {
		// return f.map(MapMode.READ_WRITE, pos, nextBlockPos - pos);
		return null;
	}

	@Override
	public synchronized File getFile() {
		return p.toFile();
	}

	@Override
	public synchronized void delete() throws IOException {
		close();
		Files.delete(p);
	}

	@Override
	public void copy(RawFile orig, long from, long len, long pos) {
		// TODO Auto-generated method stub

	}

}
