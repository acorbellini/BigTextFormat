package edu.bigtextformat.levels.levelfile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import edu.bigtextformat.block.Block;
import edu.bigtextformat.block.BlockFile;
import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.levels.DataBlock;
import edu.bigtextformat.levels.Index;
import edu.bigtextformat.levels.LevelOptions;
import edu.bigtextformat.levels.Pair;
import edu.bigtextformat.levels.PairReader;
import edu.bigtextformat.levels.SortedLevelFile;
import edu.jlime.util.DataTypeUtils;

public class LevelFile {
	private Cache<Long, DataBlock> cache = CacheBuilder.newBuilder()
			.softValues().build();

	private BlockFile file = null;

	private byte[] minKey;
	private byte[] maxKey;

	private LevelOptions opts;
	private int cont;
	private int level;
	private String dir;
	private String path;

	private boolean commited = false;

	private Index index;

	// public LevelFile(String path, LevelOptions opts, String dir, int level,
	// int cont) throws Exception {
	// this.index = new Index();
	// this.dir = dir;
	// this.opts = opts;
	// this.file = openBlockFile(path, true);
	// this.path = path;
	// this.level = level;
	// this.cont = cont;
	// }

	private LevelFile(BlockFile bf, LevelOptions opts, int level, int cont,
			Index i, byte[] minKey, byte[] maxKey) {
		this.dir = bf.getRawFile().getFile().getParent();
		this.path = bf.getRawFile().getFile().getPath();
		this.opts = opts;
		this.file = bf;
		this.level = level;
		this.cont = cont;
		this.index = i;
		this.minKey = minKey;
		this.maxKey = maxKey;
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

		Block b = file.newFixedBlock(table.toByteArray());
		index.put(table.lastKey(), b.getPos(), opts.format);
	}

	public void commit() throws Exception {
		file.newFixedBlock(index.toByteArray());
		file.getHeader().putData("minKey", minKey);
		file.getHeader().putData("maxKey", maxKey);
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
		BlockFile bf = openBlockFile(path, true);
		bf.getHeader().putData("opts", opts.toByteArray());
		return new LevelFile(bf, opts, level, cont, new Index(), null, null);
	}

	public static BlockFile openBlockFile(String path, boolean write)
			throws Exception {
		return BlockFile.open(path, 512, 512,
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
		try {
			file.close();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		File curr = file.getRawFile().getFile();
		String newPath = SortedLevelFile.getPath(dir, level + 1, cont);
		try {
			Files.move(Paths.get(curr.getPath()), Paths.get(newPath));
		} catch (Exception e) {
			e.printStackTrace();
		}
		path = newPath;
		file = openBlockFile(newPath, false);
		level = i;
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

	public Pair<byte[], byte[]> getFirstBetween(byte[] from, boolean inclFrom,
			byte[] to, boolean inclTo, BlockFormat format) throws Exception {
		if (format.compare(from, maxKey) > 0 || format.compare(to, minKey) < 0)
			return null;
		// long pos = index.get(from, format);
		// if (pos < 0)
		// return null;

		Iterator<Long> it = index.range(from, to, format);
		while (it.hasNext()) {
			Long bPos = (Long) it.next();
			DataBlock db = getDataBlock(bPos);
			Pair<byte[], byte[]> pair = db.getFirstBetween(from, inclFrom, to,
					inclTo, format);
			if (pair != null)
				return pair;
			// else
			// System.out.println("Next");
		}
		return null;
	}

	public static LevelFile open(File f) throws Exception {
		BlockFile bf = openBlockFile(f.getPath(), false);
		LevelOptions opts = new LevelOptions().fromByteArray(bf.getHeader()
				.get("opts"));
		int level = Integer.valueOf(f.getName().substring(0,
				f.getName().indexOf("-")));
		int cont = Integer.valueOf(f.getName().substring(
				f.getName().indexOf("-") + 1, f.getName().indexOf(".")));
		Index i = new Index().fromByteArray(bf.getLastBlock().payload());
		byte[] minKey = bf.getHeader().get("minKey");
		byte[] maxKey = bf.getHeader().get("maxKey");
		return new LevelFile(bf, opts, level, cont, i, minKey, maxKey);
	}
}
