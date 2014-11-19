package edu.bigtextformat.levels.levelfile;

import java.io.File;
import java.lang.ref.SoftReference;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import edu.bigtextformat.block.Block;
import edu.bigtextformat.block.BlockFile;
import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.levels.DataBlock;
import edu.bigtextformat.levels.DataBlockImpl;
import edu.bigtextformat.levels.Index;
import edu.bigtextformat.levels.LevelOptions;
import edu.bigtextformat.levels.Pair;
import edu.bigtextformat.levels.PairReader;
import edu.bigtextformat.levels.SortedLevelFile;
import edu.jlime.util.DataTypeUtils;

public class LevelFile {
	private static final long MAX_CACHE_SIZE = 1000;

	private static Cache<DataBlockID, DataBlockImpl> cache = CacheBuilder
			.newBuilder().maximumSize(MAX_CACHE_SIZE)
			.expireAfterAccess(30, TimeUnit.SECONDS).softValues().build();

	private volatile BlockFile file = null;
	private volatile LevelOptions opts = null;
	private volatile SoftReference<Index> index;

	private int cont;
	private int level;
	private String dir;
	private String path;

	private boolean commited = false;

	private boolean deleted;

	private Index fixedIndex;

	private UUID id = UUID.randomUUID();

	private LevelFile(File f, int level, int cont) {
		this.dir = f.getParent();
		this.path = f.getPath();
		this.level = level;
		this.cont = cont;
	}

	public void put(DataBlock dataBlock) throws Exception {
		if (commited)
			throw new Exception("LevelFile is read only");

		byte[] firstKey = dataBlock.firstKey();
		if (getIndex().getMinKey() == null
				|| getOpts().format.compare(getIndex().getMinKey(), firstKey) > 0)
			getIndex().setMinKey(firstKey);

		byte[] lastKey = dataBlock.lastKey();
		if (getIndex().getMaxKey() == null
				|| getOpts().format.compare(getIndex().getMaxKey(), lastKey) < 0)
			getIndex().setMaxKey(lastKey);

		long pos = 0;

		if (dataBlock.getBlock() != null)
			pos = getFile().appendBlock(dataBlock.getBlockFile(),
					dataBlock.getPos(), dataBlock.getLen());
		else {
			Block b = getFile().newFixedBlock(dataBlock.toByteArray());
			pos = b.getPos();
		}
		getIndex().put(dataBlock.lastKey(), pos, getOpts().format);
	}

	public void commit() throws Exception {
		getFile().newUncompressedFixedBlock(getIndex().toByteArray());
		getFile().close();
		fixedIndex = null;
		commited = true;
	}

	public void persist() throws Exception {
		// System.out.println("Closing" + file.getRawFile().getFile());

		File curr = getFile().getRawFile().getFile();
		String newPath = SortedLevelFile.getPath(dir, level, cont);
		// System.out.println("Moving " + curr + " to " + newPath);
		try {
			Files.move(Paths.get(curr.getPath()), Paths.get(newPath));
		} catch (Exception e) {
			e.printStackTrace();
		}
		// System.out.println("Moved " + curr + " to " + newPath);
		path = newPath;
		setFile(openBlockFile(newPath));
		// System.out.println("Persited " + newPath);
	}

	public LevelFileWriter getWriter() throws Exception {
		return new LevelFileWriter(this, getOpts());
	}

	public static LevelFile newFile(String dir, LevelOptions opts, int level,
			int cont) throws Exception {
		String path = SortedLevelFile.getTempPath(dir, level, cont);
		BlockFile bf = createBlockFile(path, opts);
		bf.getHeader().putData("opts", opts.toByteArray());
		LevelFile lf = new LevelFile(new File(path), level, cont);
		lf.setFile(bf);
		lf.setOpts(opts);
		lf.setIndex(new Index(opts.format));
		return lf;
	}

	private void setIndex(Index index2) {
		this.fixedIndex = index2;

	}

	private static BlockFile createBlockFile(String path, LevelOptions opts)
			throws Exception {
		return BlockFile.create(path, 512, 512,
				DataTypeUtils.byteArrayToLong("SSTTABLE".getBytes()),
				opts.comp, true);
	}

	public static BlockFile openBlockFile(String path) throws Exception {
		return BlockFile.open(path,
				DataTypeUtils.byteArrayToLong("SSTTABLE".getBytes()));
	}

	public int getLevel() {
		return level;
	}

	public long size() throws Exception {
		return getFile().size();
	}

	public int getCont() {
		return cont;
	}

	public byte[] getMinKey() throws Exception {
		return getIndex().getMinKey();
	}

	public byte[] getMaxKey() throws Exception {
		return getIndex().getMaxKey();
	}

	public void moveTo(int i, int cont) throws Exception {
		try {
			getFile().close();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		File curr = getFile().getRawFile().getFile();
		String newPath = SortedLevelFile.getPath(dir, i, cont);
		try {
			Files.move(Paths.get(curr.getPath()), Paths.get(newPath));
		} catch (Exception e) {
			e.printStackTrace();
		}
		path = newPath;
		setFile(openBlockFile(newPath));
		level = i;
		this.cont = cont;
	}

	public LevelFileReader getReader() throws Exception {
		return new LevelFileReader(this);
	}

	public DataBlock getDataBlock(long pos) throws Exception {
		DataBlockImpl block = cache.getIfPresent(DataBlockID.create(id, pos));
		if (block == null) {
			synchronized (cache) {
				block = cache.getIfPresent(pos);
				if (block == null) {
					Block b = getFile().getBlock(pos, false);
					if (b.getNextBlockPos() == getFile().length()) // index
																	// position...
						return null;
					block = new DataBlockImpl().fromByteArray(b.payload());
					block.setBlock(b);
					cache.put(DataBlockID.create(id, b.getPos()), block);
				}
			}
		}
		return block;
	}

	public synchronized void delete() throws Exception {
		getFile().delete();
		deleted = true;
	}

	public boolean contains(byte[] k, BlockFormat format) throws Exception {
		synchronized (this) {
			if (deleted)
				return false;
		}
		long pos = getIndex().get(k);
		if (pos < 0)
			return false;
		DataBlock db = getDataBlock(pos);
		if (db.contains(k, format))
			return true;
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

			DataBlock dataBlock = reader.next();
			builder.append(dataBlock.print(getOpts().format));

		}
		builder.append(")");
		return builder.toString();
	}

	@Override
	public String toString() {
		try {
			return "LevelFile [level=" + level + ", minKey="
					+ Arrays.toString(getIndex().getMinKey()) + ", maxKey="
					+ Arrays.toString(getIndex().getMaxKey()) + ", cont="
					+ cont + ", path=" + path + "]";
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "";
	}

	public void close() throws Exception {
		getFile().close();
	}

	public Pair<byte[], byte[]> getFirstBetween(byte[] from, boolean inclFrom,
			byte[] to, boolean inclTo, BlockFormat format) throws Exception {
		// if (format.compare(from, maxKey) > 0 || format.compare(to, minKey) <
		// 0)
		// return null;
		// long pos = index.get(from, format);
		// if (pos < 0)
		// return null;

		Iterator<Long> it = getIndex().range(from, to, format);
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

		int level = Integer.valueOf(f.getName().substring(0,
				f.getName().indexOf("-")));
		int cont = Integer.valueOf(f.getName().substring(
				f.getName().indexOf("-") + 1, f.getName().indexOf(".")));

		// BlockFile bf = openBlockFile(f.getPath(), false);
		//
		// LevelOptions opts = new LevelOptions().fromByteArray(bf.getHeader()
		// .get("opts"));
		//
		// Index i = new Index(opts.format).fromByteArray(bf.getLastBlock()
		// .payload());

		return new LevelFile(f, level, cont);
	}

	public byte[] get(byte[] k, BlockFormat format) throws Exception {
		synchronized (this) {
			if (deleted)
				return null;
		}
		long pos = getIndex().get(k);
		if (pos < 0)
			return null;
		DataBlock db = getDataBlock(pos);
		return db.get(k, format);
	}

	public BlockFile getFile() throws Exception {
		if (file == null) {
			synchronized (this) {
				if (file == null)
					file = openBlockFile(path);
			}
		}

		return file;
	}

	public void setFile(BlockFile file) {
		this.file = file;
	}

	public Index getIndex() throws Exception {
		if (fixedIndex != null)
			return fixedIndex;

		if (index == null || index.get() == null) {
			synchronized (this) {
				if (index == null || index.get() == null) {
					Index i = new Index(getOpts().format)
							.fromByteArray(getFile().getLastBlock().payload());
					index = new SoftReference<Index>(i);
				}
			}
		}
		return index.get();
	}

	public LevelOptions getOpts() throws Exception {
		if (opts == null) {
			synchronized (this) {
				if (opts == null) {
					opts = new LevelOptions().fromByteArray(getFile()
							.getHeader().get("opts"));
				}

			}
		}
		return opts;
	}

	public void setOpts(LevelOptions opts) {
		this.opts = opts;
	}
}
