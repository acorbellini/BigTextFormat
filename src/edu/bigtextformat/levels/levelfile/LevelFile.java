package edu.bigtextformat.levels.levelfile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import edu.bigtextformat.block.Block;
import edu.bigtextformat.block.BlockFile;
import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.levels.DataBlock;
import edu.bigtextformat.levels.Index;
import edu.bigtextformat.levels.LevelOptions;
import edu.bigtextformat.levels.PairReader;
import edu.bigtextformat.levels.SortedLevelFile;
import edu.jlime.util.DataTypeUtils;

public class LevelFile {
	private BlockFile file = null;
	private int level;
	private Index index;
	private boolean commited = false;
	private byte[] minKey;
	private byte[] maxKey;
	private Cache<Long, DataBlock> cache = CacheBuilder.newBuilder()
			.maximumSize(100).build();

	private LevelOptions opts;
	private int cont;
	private String dir;
	private String path;

	public LevelFile(String path, LevelOptions opts, String dir, int level,
			int cont) throws Exception {
		this.index = new Index();
		this.dir = dir;
		this.opts = opts;
		this.file = openBlockFile(path, true);
		this.path = path;
		this.level = level;
		this.cont = cont;
	}

	public void put(DataBlock table) throws Exception {
		if (commited)
			throw new Exception("LevelFile is read only");

		byte[] firstKey = table.firstKey();
		if (minKey == null || opts.format.compare(minKey, firstKey) > 0)
			minKey = firstKey;

		byte[] lastKey = table.lastKey();
		if (maxKey == null || opts.format.compare(maxKey, lastKey) < 0)
			maxKey = lastKey;

		Block b = file.newBlock(table.toByteArray());
		index.put(table.lastKey(), b.getPos(), opts.format);
	}

	public void commit() throws Exception {
		file.newBlock(index.toByteArray());
		file.close();
		commited = true;
	}

	public void persist() throws Exception {
		// System.out.println("Closing" + file.getRawFile().getFile());

		File curr = file.getRawFile().getFile();
		String newPath = SortedLevelFile.getPath(dir, level, cont);
		// System.out.println("Moving " + curr + " to " + newPath);
		try {
			Files.move(Paths.get(curr.getPath()), Paths.get(newPath));
		} catch (Exception e) {
			e.printStackTrace();
		}
		// System.out.println("Moved " + curr + " to " + newPath);
		path = newPath;
		file = openBlockFile(newPath, false);
		// System.out.println("Persited " + newPath);
	}

	public LevelFileWriter getWriter() {
		return new LevelFileWriter(this, opts);
	}

	public static LevelFile newFile(String dir, LevelOptions opts, int level,
			int cont) throws Exception {
		String path = SortedLevelFile.getTempPath(dir, level, cont);
		return new LevelFile(path, opts, dir, level, cont);
	}

	public static BlockFile openBlockFile(String path, boolean write)
			throws Exception {
		return BlockFile.open(path, 32, 512,
				DataTypeUtils.byteArrayToLong("SSTTABLE".getBytes()), true,
				write, write);
	}

	public int getLevel() {
		return level;
	}

	public long size() throws Exception {
		return file.size();
	}

	public int getCont() {
		return cont;
	}

	public byte[] getMinKey() {
		return minKey;
	}

	public byte[] getMaxKey() {
		return maxKey;
	}

	public void moveTo(int i, int cont) throws Exception {
		File curr = file.getRawFile().getFile();
		String newPath = SortedLevelFile.getPath(dir, level + 1, cont);
		try {
			Files.move(Paths.get(curr.getPath()), Paths.get(newPath));
			// Files.move(curr, new File(newPath));
		} catch (Exception e) {
			e.printStackTrace();
		}
		path = newPath;
		file = openBlockFile(newPath, false);
		level = level + 1;
		this.cont = cont;
	}

	public LevelFileReader getReader() throws Exception {
		return new LevelFileReader(this);
	}

	public BlockFile getBlockFile() {
		return file;
	}

	public DataBlock getDataBlock(long pos) throws Exception {
		DataBlock block = cache.getIfPresent(pos);
		if (block == null) {
			synchronized (cache) {
				block = cache.getIfPresent(pos);
				if (block == null) {
					Block b = file.getBlock(pos, false);
					if (b.getNextBlockPos() == file.length()) // index
																// position...
						return null;
					block = new DataBlock().fromByteArray(b.payload());
					block.setBlock(b);
					cache.put(b.getPos(), block);
				}
			}
		}
		return block;
	}

	public void delete() throws IOException {
		file.delete();
	}

	public boolean contains(byte[] k, BlockFormat format) throws Exception {
		if (format.compare(k, minKey) >= 0 && format.compare(k, maxKey) <= 0) {
			long pos = index.get(k, format);
			if (pos < 0)
				return false;
			DataBlock db = getDataBlock(pos);
			if (db.contains(k, format))
				return true;
		}
		return false;
	}

	public void commitAndPersist() throws Exception {
		commit();
		persist();
	}

	public PairReader getPairReader() throws Exception {
		return new PairReader(this);
	}

	public String print(BlockFormat format) throws Exception {
		StringBuilder builder = new StringBuilder();
		builder.append(" " + getCont() + " : (");
		LevelFileReader reader = getReader();
		boolean first = true;
		while (reader.hasNext()) {
			if (first)
				first = false;
			else
				builder.append(" - ");

			DataBlock dataBlock = (DataBlock) reader.next();
			builder.append(dataBlock.print(opts.format));

		}
		builder.append(")");
		return builder.toString();
	}

	@Override
	public String toString() {
		return "LevelFile [level=" + level + ", minKey="
				+ Arrays.toString(minKey) + ", maxKey="
				+ Arrays.toString(maxKey) + ", cont=" + cont + ", path=" + path
				+ "]";
	}

	public void close() throws IOException {
		file.close();
	}

}
