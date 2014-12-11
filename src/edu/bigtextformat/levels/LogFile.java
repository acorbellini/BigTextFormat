package edu.bigtextformat.levels;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;

import edu.bigtextformat.block.Block;
import edu.bigtextformat.block.BlockFile;
import edu.bigtextformat.block.BlockFileOptions;
import edu.jlime.util.DataTypeUtils;

public class LogFile implements Iterable<byte[]> {

	private static final long MAGIC = DataTypeUtils.byteArrayToLong("LOGFILE_"
			.getBytes());
	private static final int READ = 0;
	private static final int APPEND = 1;
	private BlockFile file;
	private String p;
	private String name;
	private int mode = -1;

	public LogFile(File filename) throws Exception {
		this.p = filename.getPath();
		this.name = filename.getName();
	}

	public void appendMode() throws Exception {
		if (mode == APPEND)
			return;
		if (file != null)
			file.close();
		file = createLog(p);
		mode = APPEND;
	}

	private BlockFile createLog(String path) throws Exception {
		return BlockFile.create(path, new BlockFileOptions().setMagic(MAGIC)
				.setEnableCache(false).setAppendOnly(true)
		// .setComp(CompressionType.SNAPPY.getComp())
				);
	}

	public void append(byte[] opAsBytes) throws Exception {
		file.newFixedBlock(opAsBytes);
	}

	public String getName() {
		return name;
	}

	public void close() throws IOException {
		if (file != null)
			file.close();
		file = null;
	}

	public byte[] get(byte[] k) throws Exception {
		readMode();
		return null;
	}

	synchronized void readMode() throws Exception {
		if (mode == READ)
			return;
		close();
		file = BlockFile.open(p, MAGIC);
		mode = READ;
	}

	@Override
	public Iterator<byte[]> iterator() {
		try {
			readMode();
		} catch (Exception e) {
			e.printStackTrace();
		}

		if (file == null)
			return null;

		final Iterator<Block> it = file.iterator();
		return new Iterator<byte[]>() {

			@Override
			public boolean hasNext() {
				return it.hasNext();
			}

			@Override
			public byte[] next() {
				return it.next().payload();
			}

			@Override
			public void remove() {

			}

		};

	}

	public long size() throws Exception {
		return file.size();
	}

	public synchronized void delete() throws IOException {
		if (file != null)
			file.delete();
		else
			Files.delete(Paths.get(p));
	}

	public boolean isEmpty() throws Exception {
		return file.isEmpty();
	}

	public void flush() throws IOException {
		file.flush();
	}
}
