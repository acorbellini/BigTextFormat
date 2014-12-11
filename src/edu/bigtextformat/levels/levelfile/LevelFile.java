package edu.bigtextformat.levels.levelfile;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import edu.bigtextformat.block.Block;
import edu.bigtextformat.block.BlockFile;
import edu.bigtextformat.block.BlockFileOptions;
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

	private static Timer timer = new Timer("LevelFile close daemon", true);

	private static enum LevelFileStatus {
		CREATED, COMMITED, PERSISTED, DELETED, MERGING
	}

	ReadWriteLock closeLock = new ReentrantReadWriteLock();

	private static final long MAX_CACHE_SIZE = 100;

	private static final long TIME_TO_CLOSE = 5000;

	private static Cache<DataBlockID, DataBlock> cache = CacheBuilder
			.newBuilder().concurrencyLevel(10).maximumSize(MAX_CACHE_SIZE)
			.softValues().build();

	private volatile LevelFileStatus state = LevelFileStatus.PERSISTED;

	private volatile BlockFile file = null;

	private volatile LevelOptions opts = null;

	private volatile Index index;

	private int cont;
	private int level;

	private String dir;
	private String path;

	private byte[] minKey;
	private byte[] maxKey;

	private Index fixedIndex;

	private UUID id = UUID.randomUUID();

	private String name;

	private volatile long lastUsed;

	private Lock rl;

	private Lock wl;

	private TimerTask closeTask;

	private LevelFile(File f, int level, int cont, byte[] minKey, byte[] maxKey) {
		this.dir = f.getParent();
		this.path = f.getPath();
		this.name = f.getName();
		this.level = level;
		this.cont = cont;
		this.minKey = minKey;
		this.maxKey = maxKey;
		this.rl = closeLock.readLock();
		this.wl = closeLock.writeLock();
	}

	public String getName() {
		return name;
	}

	public void put(DataBlock dataBlock) throws Exception {
		if (!state.equals(LevelFileStatus.CREATED))
			throw new Exception("LevelFile is read only");

		byte[] firstKey = dataBlock.firstKey();
		if (minKey == null || getOpts().format.compare(minKey, firstKey) > 0)
			this.minKey = firstKey;
		byte[] lastKey = dataBlock.lastKey();
		if (maxKey == null || getOpts().format.compare(maxKey, lastKey) < 0) {
			this.maxKey = lastKey;
		}

		long pos = 0;

		if (dataBlock.getBlockPos() != null) {
			LevelFile otherFile = dataBlock.getFile();
			otherFile.rl.lock();
			BlockFile other = otherFile.getFile();
			pos = getFile().appendBlock(other, dataBlock.getBlockPos(),
					dataBlock.getLen());
			otherFile.rl.unlock();
		} else {
			Block b = getFile().newFixedBlock(dataBlock.toByteArray());
			pos = b.getPos();
		}
		getIndex().put(dataBlock.lastKey(), pos, getOpts().format);
	}

	public void commit() throws Exception {
		getIndex().setMinKey(minKey);
		getIndex().setMaxKey(maxKey);
		getFile().newFixedBlock(getIndex().toByteArray());
		getFile().close();
		fixedIndex = null;
		state = LevelFileStatus.COMMITED;
	}

	public synchronized void persist() throws Exception {
		// System.out.println("Closing" + file.getRawFile().getFile());

		// File curr = getFile().getRawFile().getFile();
		String newPath = SortedLevelFile.getPath(dir, level, cont);
		// System.out.println("Moving " + curr + " to " + newPath);
		try {
			Files.move(Paths.get(path), Paths.get(newPath));
		} catch (Exception e) {
			e.printStackTrace();
		}
		// System.out.println("Moved " + curr + " to " + newPath);
		path = newPath;
		name = new File(newPath).getName();
		file = null;
		state = LevelFileStatus.PERSISTED;
		startTimer();
		// setFile(openBlockFile(newPath));
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
		LevelFile lf = new LevelFile(new File(path), level, cont, null, null);
		lf.setFile(bf);
		lf.setOpts(opts);
		lf.state = LevelFileStatus.CREATED;
		lf.setIndex(new Index(opts.format));
		return lf;
	}

	private void setIndex(Index index2) {
		this.fixedIndex = index2;

	}

	private static BlockFile createBlockFile(String path, LevelOptions opts)
			throws Exception {
		return BlockFile.create(
				path,
				new BlockFileOptions()
						.setHeaderSize(512)
						.setMinSize(512)
						.setAppendOnly(true)
						.setMagic(
								DataTypeUtils.byteArrayToLong("SSTTABLE"
										.getBytes())).setComp(opts.comp));
	}

	public static BlockFile openBlockFile(String path) throws Exception {
		return BlockFile.open(path,
				DataTypeUtils.byteArrayToLong("SSTTABLE".getBytes()));
	}

	public int getLevel() {
		return level;
	}

	public long size() throws Exception {
		try {
			rl.lock();
			return getFile().size();
		} finally {
			rl.unlock();
		}
	}

	public int getCont() {
		return cont;
	}

	public byte[] getMinKey() throws Exception {
		if (minKey != null)
			return minKey;
		else
			minKey = getIndex().getMinKey();
		return minKey;
	}

	public byte[] getMaxKey() throws Exception {
		if (maxKey != null)
			return maxKey;
		else
			maxKey = getIndex().getMaxKey();
		return maxKey;
	}

	public void moveTo(int i, int cont) throws Exception {
		try {
			close();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		String newPath = SortedLevelFile.getPath(dir, i, cont);
		try {
			Files.move(Paths.get(path), Paths.get(newPath));
		} catch (Exception e) {
			e.printStackTrace();
		}
		path = newPath;
		name = new File(newPath).getName();
		// setFile(openBlockFile(newPath));
		level = i;
		this.cont = cont;
	}

	public LevelFileReader getReader() throws Exception {
		return new LevelFileReader(this);
	}

	public DataBlock getDataBlock(final long pos) throws Exception {
		DataBlock block = cache.get(DataBlockID.create(id, pos),
				new Callable<DataBlock>() {

					@Override
					public DataBlock call() throws Exception {
						rl.lock();
						Block b;
						long length;
						try {
							b = getFile().getBlock(pos, false);
							length = getFile().length();
						} finally {
							rl.unlock();
						}
						if (b.getNextBlockPos() == length) // index
															// position...
							throw new Exception(
									"Trying to read index position.");
						DataBlock block = new DataBlockImpl(LevelFile.this, b
								.getPos(), b.size()).fromByteArray(b.payload());
						// cache.put(DataBlockID.create(id, b.getPos()), block);
						return block;
					}
				});
		return block;
	}

	public synchronized void delete() throws Exception {
		wl.lock();
		try {
			getFile().delete();
			file = null;
		} catch (Exception e) {
			Files.delete(Paths.get(path));
			e.printStackTrace();
		} finally {
			state = LevelFileStatus.DELETED;
			wl.unlock();
		}

	}

	public boolean contains(byte[] k, BlockFormat format) throws Exception {
		synchronized (this) {
			if (state.equals(LevelFileStatus.DELETED))
				return false;
		}

		if (getMinKey() != null && getMaxKey() != null) {
			if (format.compare(k, getMinKey()) < 0
					|| format.compare(k, getMaxKey()) > 0)
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

	public String print() throws Exception {
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
			return "LevelFile [level=" + level + ", minKey=" + minKey + ","
					+ minKey != null ? Arrays.toString(minKey) : "null"
					+ " maxKey=" + maxKey != null ? Arrays.toString(maxKey)
					: "null" + ", cont=" + cont + ", path=" + path + "]";
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "";
	}

	public synchronized void close() throws Exception {
		wl.lock();
		try {
			if (file != null)
				file.close();
			file = null;
			index = null;
			if (closeTask != null) {
				closeTask.cancel();
				closeTask = null;
			}
		} finally {
			wl.unlock();
		}

	}

	public Pair<byte[], byte[]> getFirstBetween(byte[] from, boolean inclFrom,
			byte[] to, boolean inclTo) throws Exception {
		// lastUsed = System.currentTimeMillis();
		// if (format.compare(from, maxKey) > 0 || format.compare(to, minKey) <
		// 0)
		// return null;
		// long pos = index.get(from, format);
		// if (pos < 0)
		// return null;
		int compareMin = getOpts().format.compare(to, minKey);

		if (compareMin < 0 || (compareMin == 0 && !inclTo))
			return null;

		int compareMax = getOpts().format.compare(from, maxKey);
		if (compareMax > 0 || (compareMax == 0 && !inclFrom))
			return null;

		Iterator<Long> it = getIndex().range(from, to, getOpts().format);
		while (it.hasNext()) {
			Long bPos = (Long) it.next();
			DataBlock db = getDataBlock(bPos);
			Pair<byte[], byte[]> pair = db.getFirstBetween(from, inclFrom, to,
					inclTo, getOpts().format);
			if (pair != null)
				return pair;
			// else
			// System.out.println("Next");
		}
		return null;
	}

	// public static LevelFile open(File f) throws Exception {
	//
	// int level = Integer.valueOf(f.getName().substring(0,
	// f.getName().indexOf("-")));
	// int cont = Integer.valueOf(f.getName().substring(
	// f.getName().indexOf("-") + 1, f.getName().indexOf(".")));
	//
	// // BlockFile bf = openBlockFile(f.getPath(), false);
	// //
	// // LevelOptions opts = new LevelOptions().fromByteArray(bf.getHeader()
	// // .get("opts"));
	// //
	// // Index i = new Index(opts.format).fromByteArray(bf.getLastBlock()
	// // .payload());
	//
	// return new LevelFile(f, level, cont);
	// }

	public byte[] get(byte[] k, BlockFormat format) throws Exception {
		// lastUsed = System.currentTimeMillis();
		synchronized (this) {
			if (state.equals(LevelFileStatus.DELETED))
				return null;
		}

		if (getMinKey() != null && getMaxKey() != null) {
			if (format.compare(k, getMinKey()) < 0
					|| format.compare(k, getMaxKey()) > 0)
				return null;
		}

		long pos = getIndex().get(k);
		if (pos < 0)
			return null;
		DataBlock db = getDataBlock(pos);
		return db.get(k, format);
	}

	private BlockFile getFile() throws Exception {
		lastUsed = System.currentTimeMillis();

		if (file == null) {
			synchronized (this) {
				if (file == null) {
					file = openBlockFile(path);
					if (!state.equals(LevelFileStatus.CREATED))
						startTimer();
				}
			}
		}
		return file;
	}

	private void startTimer() {
		closeTask = new TimerTask() {
			@Override
			public void run() {
				try {
					long currentTimeMillis = System.currentTimeMillis();
					if (currentTimeMillis - lastUsed >= TIME_TO_CLOSE) {
						close();
						cancel();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		};
		timer.schedule(closeTask, TIME_TO_CLOSE, TIME_TO_CLOSE);
	}

	private void setFile(BlockFile file) {
		this.file = file;
	}

	public Index getIndex() throws Exception {
		// lastUsed = System.currentTimeMillis();
		if (fixedIndex != null)
			return fixedIndex;

		if (index == null) {
			// || index.get() == null) {
			synchronized (this) {
				if (index == null) {
					// || index.get() == null) {
					// Index i = new Index(getOpts().format)
					// .fromByteArray(getFile().getLastBlock().payload());
					// index = new SoftReference<Index>(i);
					rl.lock();
					byte[] payload;
					try {
						payload = getFile().getLastBlock().payload();
					} finally {
						rl.unlock();
					}

					index = new Index(getOpts().format).fromByteArray(payload);
				}
			}
		}
		return index;
		// .get();
	}

	public LevelOptions getOpts() throws Exception {
		// lastUsed = System.currentTimeMillis();
		if (opts == null) {
			synchronized (this) {
				if (opts == null) {
					rl.lock();
					byte[] data;
					try {
						data = getFile().getHeader().get("opts");
					} finally {
						rl.unlock();
					}

					opts = new LevelOptions().fromByteArray(data);

				}

			}
		}
		return opts;
	}

	public void setOpts(LevelOptions opts) {
		this.opts = opts;
	}

	public static LevelFile open(int level, int cont, String levelFileName,
			byte[] minKey, byte[] maxKey) {
		return new LevelFile(new File(levelFileName), level, cont, minKey,
				maxKey);
	}

	public String getPath() {
		return path;
	}

	public synchronized boolean isMerging() {
		return state.equals(LevelFileStatus.MERGING);
	}

	public synchronized boolean setMerging(int level) {
		if (isDeleted() || isMerging() || this.level != level)
			return false;
		this.state = LevelFileStatus.MERGING;
		return true;
	}

	public synchronized void unSetMerging() {
		this.state = LevelFileStatus.PERSISTED;
	}

	public boolean isDeleted() {
		return this.state.equals(LevelFileStatus.DELETED);
	}

	public long getLastBlockPosition() throws Exception {
		try {
			rl.lock();
			return getFile().getLastBlockPosition();
		} finally {
			rl.unlock();
		}
	}

	public boolean intersectsWith(LevelFile other) throws Exception {
		return opts.format.compare(getMinKey(), other.getMaxKey()) <= 0
				&& getOpts().format.compare(getMaxKey(), other.getMinKey()) >= 0;
	}
}
