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
import edu.bigtextformat.levels.LevelOptions;
import edu.bigtextformat.levels.PairReader;
import edu.bigtextformat.levels.SortedLevelFile;
import edu.bigtextformat.levels.datablock.DataBlock;
import edu.bigtextformat.levels.datablock.DataBlockImpl;
import edu.bigtextformat.levels.index.Index;
import edu.bigtextformat.util.Pair;
import edu.jlime.util.DataTypeUtils;

public class LevelFile {

	private static enum LevelFileStatus {
		CREATED, COMMITED, PERSISTED, DELETED, MERGING
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

	public static LevelFile open(int level, int cont, String levelFileName,
			byte[] minKey, byte[] maxKey) {
		return new LevelFile(new File(levelFileName), level, cont, minKey,
				maxKey);
	}

	public static BlockFile openBlockFile(String path) throws Exception {
		return BlockFile.open(path,
				DataTypeUtils.byteArrayToLong("SSTTABLE".getBytes()));
	}

	private static Timer timer = new Timer("LevelFile close daemon", true);

	ReadWriteLock closeLock = new ReentrantReadWriteLock();

	private static final long MAX_CACHE_SIZE = 100;

	private static final long TIME_TO_CLOSE = 5000;

	private Cache<DataBlockID, DataBlock> cache = CacheBuilder.newBuilder()
			.maximumSize(MAX_CACHE_SIZE)
			.expireAfterAccess(1000, TimeUnit.SECONDS).softValues().build();

	private volatile LevelFileStatus state = LevelFileStatus.PERSISTED;

	private volatile BlockFile file = null;

	private volatile LevelOptions opts = null;

	private volatile Index index;

	private int cont;
	private volatile int level;

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

	public void commit() throws Exception {
		getIndex().setMinKey(minKey);
		getIndex().setMaxKey(maxKey);
		getFile().newFixedBlock(getIndex().toByteArray());
		getFile().close();
		fixedIndex = null;
		state = LevelFileStatus.COMMITED;
	}

	public void commitAndPersist() throws Exception {
		commit();
		persist();
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
		DataBlock db = getDataBlock(pos, true);
		if (db.contains(k, format))
			return true;
		return false;
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

	public byte[] get(byte[] k, BlockFormat format) throws Exception {
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
		DataBlock db = getDataBlock(pos, true);
		return db.get(k, format);
	}

	public int getCont() {
		return cont;
	}

	public DataBlock getDataBlock(final long pos, boolean saveInCache)
			throws Exception {
		if (!saveInCache)
			return readDataBlock(pos);
		DataBlock block = cache.get(DataBlockID.create(id, pos),
				new Callable<DataBlock>() {

					@Override
					public DataBlock call() throws Exception {
						return readDataBlock(pos);
					}

				});
		return block;
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

	public Pair<byte[], byte[]> getFirstBetween(byte[] from, boolean inclFrom,
			byte[] to, boolean inclTo) throws Exception {
		int compareMin = getOpts().format.compare(to, minKey);

		if (compareMin < 0 || (compareMin == 0 && !inclTo))
			return null;

		int compareMax = getOpts().format.compare(from, maxKey);
		if (compareMax > 0 || (compareMax == 0 && !inclFrom))
			return null;

		Iterator<Long> it = getIndex().range(from, to, getOpts().format);
		while (it.hasNext()) {
			Long bPos = (Long) it.next();
			DataBlock db = getDataBlock(bPos, true);
			Pair<byte[], byte[]> pair = db.getFirstBetween(from, inclFrom, to,
					inclTo, getOpts().format);
			if (pair != null)
				return pair;
		}
		return null;
	}

	public Index getIndex() throws Exception {
		if (fixedIndex != null)
			return fixedIndex;

		if (index == null) {
			synchronized (this) {
				if (index == null) {
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
	}

	public long getLastBlockPosition() throws Exception {
		try {
			rl.lock();
			return getFile().getLastBlockPosition();
		} finally {
			rl.unlock();
		}
	}

	public int getLevel() {
		return level;
	}

	public byte[] getMaxKey() throws Exception {
		if (maxKey != null)
			return maxKey;
		else
			maxKey = getIndex().getMaxKey();
		return maxKey;
	}

	public byte[] getMinKey() throws Exception {
		if (minKey != null)
			return minKey;
		else
			minKey = getIndex().getMinKey();
		return minKey;
	}

	public String getName() {
		return name;
	}

	public LevelOptions getOpts() throws Exception {
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

	public PairReader getPairReader() throws Exception {
		return new PairReader(this);
	}

	public String getPath() {
		return path;
	}

	public LevelFileReader getReader() throws Exception {
		return new LevelFileReader(this);
	}

	public LevelFileWriter getWriter() throws Exception {
		return new LevelFileWriter(this, getOpts());
	}

	public boolean intersectsWith(LevelFile other) throws Exception {
		return opts.format.compare(getMinKey(), other.getMaxKey()) <= 0
				&& getOpts().format.compare(getMaxKey(), other.getMinKey()) >= 0;
	}

	public boolean isDeleted() {
		return this.state.equals(LevelFileStatus.DELETED);
	}

	public synchronized boolean isMerging() {
		return state.equals(LevelFileStatus.MERGING);
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
		level = i;
		this.cont = cont;
	}

	public synchronized void persist() throws Exception {
		String newPath = SortedLevelFile.getPath(dir, level, cont);
		try {
			Files.move(Paths.get(path), Paths.get(newPath));
		} catch (Exception e) {
			e.printStackTrace();
		}
		path = newPath;
		name = new File(newPath).getName();
		file = null;
		state = LevelFileStatus.PERSISTED;
		startTimer();
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

	private DataBlock readDataBlock(final long pos) throws Exception {
		rl.lock();
		Block b;
		long length;
		try {
			b = getFile().getBlock(pos, false);
			length = getFile().length();
		} finally {
			rl.unlock();
		}
		if (b.getNextBlockPos() == length)
			throw new Exception("Trying to read index position.");
		DataBlock block = new DataBlockImpl(LevelFile.this, b.getPos(),
				b.size()).fromByteArray(b.payload());
		return block;
	}

	private void setFile(BlockFile file) {
		this.file = file;
	}

	private void setIndex(Index index2) {
		this.fixedIndex = index2;

	}

	public synchronized boolean setMerging(int level) {
		if (isDeleted() || isMerging() || this.level != level)
			return false;
		this.state = LevelFileStatus.MERGING;
		return true;
	}

	public void setOpts(LevelOptions opts) {
		this.opts = opts;
	}

	public long size() throws Exception {
		try {
			rl.lock();
			return getFile().size();
		} finally {
			rl.unlock();
		}
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

	public synchronized void unSetMerging() {
		if (isMerging())
			this.state = LevelFileStatus.PERSISTED;
	}
}
