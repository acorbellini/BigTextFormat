package edu.bigtextformat.levels.levelfile;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

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
		BlockFileOptions blockopts = new BlockFileOptions().setHeaderSize(512)
				.setAppendOnly(true)
				.setMagic(DataTypeUtils.byteArrayToLong("SSTTABLE".getBytes()))
				.setComp(opts.comp);
		Map<String, byte[]> header = new HashMap<>();
		header.put("opts", opts.toByteArray());
		return BlockFile.create(path, blockopts, header);
	}

	public static LevelFile newFile(String dir, LevelOptions opts, int level,
			int cont, SortedLevelFile db) throws Exception {
		String path = SortedLevelFile.getTempPath(dir, level, cont);

		BlockFile bf = createBlockFile(path, opts);

		LevelFile lf = new LevelFile(new File(path), level, cont, null, null,
				db);
		lf.setFile(bf);
		lf.setOpts(opts);
		lf.state = LevelFileStatus.CREATED;
		lf.setIndex(new Index(opts.format));
		return lf;
	}

	public static LevelFile open(int level, int cont, String levelFileName,
			byte[] minKey, byte[] maxKey, SortedLevelFile db) {
		return new LevelFile(new File(levelFileName), level, cont, minKey,
				maxKey, db);
	}

	public static BlockFile openBlockFile(String path) throws Exception {
		return BlockFile.open(path,
				DataTypeUtils.byteArrayToLong("SSTTABLE".getBytes()));
	}

	private Logger log = Logger.getLogger(LevelFile.class);

	private static Timer timer = new Timer("LevelFile close daemon", true);

	ReadWriteLock closeLock = new ReentrantReadWriteLock();

	private static final long MAX_CACHE_SIZE = 100;

	private static final long TIME_TO_CLOSE = 5000;

	private Cache<DataBlockID, DataBlock> cache = null;

	private volatile LevelFileStatus state = LevelFileStatus.PERSISTED;

	private volatile Object stateLock = new Object();

	private volatile BlockFile file = null;

	private volatile Object fileLock = new Object();

	private volatile LevelOptions opts = null;

	private volatile Object optsLock = new Object();

	private volatile Index index;

	private volatile Object indexLock = new Object();

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

	public SortedLevelFile db;

	private LevelFile(File f, int level, int cont, byte[] minKey,
			byte[] maxKey, SortedLevelFile db) {
		this.dir = f.getParent();
		this.path = f.getPath();
		this.name = f.getName();
		this.level = level;
		this.cont = cont;
		this.minKey = minKey;
		this.maxKey = maxKey;
		this.rl = closeLock.readLock();
		this.wl = closeLock.writeLock();
		this.db = db;
	}

	private Cache<DataBlockID, DataBlock> getCache() {
		if (cache == null) {
			synchronized (this) {
				if (cache == null) {
					cache = CacheBuilder.newBuilder()
							.maximumSize(MAX_CACHE_SIZE)
							.expireAfterAccess(500, TimeUnit.MILLISECONDS)
							.build();
				}
			}
		}
		return cache;
	}

	public synchronized void close() throws Exception {
		// if (cache != null)
		// cache = null;
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
		getFile().newBlock(getIndex().toByteArray(), true);
		getFile().close();
		fixedIndex = null;
		synchronized (stateLock) {
			state = LevelFileStatus.COMMITED;
		}
	}

	public void commitAndPersist() throws Exception {
		commit();
		persist();
	}

	public boolean contains(byte[] k, BlockFormat format) throws Exception {
		synchronized (stateLock) {
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
		if (db.contains(k))
			return true;
		return false;
	}

	public void delete() throws Exception {
		wl.lock();
		try {
			getFile().delete();
			file = null;
		} catch (Exception e) {
			Files.delete(Paths.get(path));
			e.printStackTrace();
		} finally {
			synchronized (stateLock) {
				state = LevelFileStatus.DELETED;
			}
			wl.unlock();
		}

	}

	public byte[] get(byte[] k, BlockFormat format) throws Exception {
		synchronized (stateLock) {
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
		return db.get(k);
	}

	public int getCont() {
		return cont;
	}

	public DataBlock getDataBlock(final long pos, boolean saveInCache)
			throws Exception {
		if (!saveInCache)
			return readDataBlock(pos);
		DataBlock block = getCache().get(DataBlockID.create(id, pos),
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
			synchronized (fileLock) {
				if (file == null) {
					file = openBlockFile(path);
					if (!isCreated())
						startTimer();
				}
			}
		}
		return file;
	}

	private boolean isCreated() {
		synchronized (stateLock) {
			return state.equals(LevelFileStatus.CREATED);
		}
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
					inclTo);
			if (pair != null)
				return pair;
		}
		return null;
	}

	public Index getIndex() throws Exception {
		if (fixedIndex != null)
			return fixedIndex;

		if (index == null) {
			synchronized (indexLock) {
				if (index == null) {
					rl.lock();
					byte[] payload = null;
					try {
						payload = getFile().getLastBlock().payload();
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						rl.unlock();
					}
					if (payload == null) {
						log.info("Starting recovery of file " + path);
						recovery();
						payload = getFile().getLastBlock().payload();
					}
					index = new Index(getOpts().format).fromByteArray(payload);
				}
			}
		}
		return index;
	}

	private void recovery() throws Exception {
		wl.lock();
		try {
			List<DataBlock> list = new ArrayList<DataBlock>();
			try {
				for (Block block : getFile()) {
					DataBlockImpl db = new DataBlockImpl(this, block.getPos(),
							block.size(), getOpts().format).fromByteArray(block
							.payload());
					list.add(db);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

			LevelFile file = LevelFile.newFile(dir, opts, level,
					db.getLastLevelIndex(level), db);

			for (DataBlock dataBlock : list)
				file.put(dataBlock);

			file.commit();

			getFile().close();
			this.file = null;
			this.index = null;
			this.cache = null;

			Files.move(Paths.get(file.path), Paths.get(path),
					StandardCopyOption.REPLACE_EXISTING);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			wl.unlock();
		}
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
			synchronized (optsLock) {
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
		return intersectsWith(other.getMinKey(), other.getMaxKey());
	}

	public boolean isDeleted() {
		synchronized (stateLock) {
			return this.state.equals(LevelFileStatus.DELETED);

		}
	}

	public boolean isMerging() {
		synchronized (stateLock) {
			return state.equals(LevelFileStatus.MERGING);
		}
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
		synchronized (stateLock) {
			state = LevelFileStatus.PERSISTED;
		}
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
		if (!isCreated())
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
			Block b = getFile().newBlock(dataBlock.toByteArray(), true);
			pos = b.getPos();
		}
		getIndex().put(dataBlock.lastKey(), pos, getOpts().format);
	}

	private DataBlock readDataBlock(final long pos) throws Exception {
		rl.lock();
		Block b;
		long length;
		try {
			b = getFile().getBlock(pos);
			length = getFile().length();
		} finally {
			rl.unlock();
		}
		if (b.getNextBlockPos() == length)
			throw new Exception("Trying to read index position.");
		DataBlock block = new DataBlockImpl(LevelFile.this, b.getPos(),
				b.size(), getOpts().format).fromByteArray(b.payload());
		return block;
	}

	private void setFile(BlockFile file) {
		this.file = file;
	}

	private void setIndex(Index index2) {
		this.fixedIndex = index2;

	}

	public boolean setMerging(int level) {
		synchronized (stateLock) {
			if (isDeleted() || isMerging() || this.level != level)
				return false;
			this.state = LevelFileStatus.MERGING;
			return true;
		}

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
			return "LevelFile [level=" + level + ", minKey="
					+ (minKey != null ? Arrays.toString(minKey) : "null")
					+ " maxKey="
					+ (maxKey != null ? Arrays.toString(maxKey) : "null")
					+ ", cont=" + cont + ", path=" + path + "]";
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "";
	}

	public void unSetMerging() {
		synchronized (stateLock) {
			if (isMerging())
				this.state = LevelFileStatus.PERSISTED;
		}
	}

	public boolean intersectsWith(byte[] minKey, byte[] maxKey)
			throws Exception {
		return opts.format.compare(getMinKey(), maxKey) <= 0
				&& getOpts().format.compare(getMaxKey(), minKey) >= 0;
	}

	public SortedLevelFile getDb() {
		return db;
	}
}
