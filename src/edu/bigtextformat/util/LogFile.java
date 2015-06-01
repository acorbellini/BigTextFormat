package edu.bigtextformat.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

import edu.bigtextformat.raw.RawFile;

public class LogFile
// implements Iterable<byte[]>
{

	private static final byte[] MAGIC = "LOGFILE_".getBytes();
	private static final int READ = 0;
	private static final int APPEND = 1;
	private RawFile file;
	private String p;
	private String name;
	private int mode = -1;

	public RawFile getFile() {
		return file;
	}

	public LogFile(File filename) throws Exception {
		this.p = filename.getPath();
		this.name = filename.getName();

	}

	public void append(byte[]... toWrite) throws Exception {
		file.append(toWrite);
	}

	public void appendMode(boolean syncmem) throws Exception {
		if (mode == APPEND)
			return;
		if (file != null)
			file.close();
		file = createLog(p, syncmem);
		mode = APPEND;
	}

	public synchronized void close() throws IOException {
		if (file != null)
			file.close();
		file = null;
	}

	private RawFile createLog(String path, boolean syncmem) throws Exception {
		RawFile file = new RawFile(path, false, false, true, syncmem);
		file.write(MAGIC);
		return file;
		// return BlockFile.create(path, new BlockFileOptions().setMagic(MAGIC)
		// .setEnableCache(false).setAppendOnly(true), null
		// .setComp(CompressionType.SNAPPY.getComp())
		// );
	}

	public synchronized void delete() throws IOException {
		if (file != null)
			file.delete();
		else
			Files.delete(Paths.get(p));
	}

	public synchronized void flush() throws IOException {
		if (file != null)
			file.sync();
	}

	public byte[] get(byte[] k) throws Exception {
		readMode();
		return null;
	}

	public String getName() {
		return name;
	}

	public boolean isEmpty() throws Exception {
		return file.length() == 0;
	}

	//
	// @Override
	// public Iterator<byte[]> iterator() {
	// try {
	// readMode();
	// } catch (Exception e) {
	// e.printStackTrace();
	// }
	//
	// if (file == null)
	// return null;
	//
	// final Iterator<Block> it = file.iterator();
	// return new Iterator<byte[]>() {
	//
	// @Override
	// public boolean hasNext() {
	// return it.hasNext();
	// }
	//
	// @Override
	// public byte[] next() {
	// return it.next().payload();
	// }
	//
	// @Override
	// public void remove() {
	//
	// }
	//
	// };
	//
	// }

	public synchronized void readMode() throws Exception {
		if (mode == READ)
			return;
		close();
		file = new RawFile(p, false, true, false, false);
		byte[] magic = file.readBytes(0, 8);
		if (!Arrays.equals(magic, MAGIC))
			throw new Exception("LogFile magic numbers do not match.");
		mode = READ;
	}

	public long size() throws Exception {
		return file.length();
	}
}
