package edu.bigtextformat.levels;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.levels.compactor.CompactWriter;
import edu.bigtextformat.levels.compactor.Compactor;
import edu.bigtextformat.levels.levelfile.LevelFile;
import edu.bigtextformat.levels.memtable.Memtable;
import edu.bigtextformat.logfile.LogFile;
import edu.bigtextformat.manifest.Manifest;
import edu.bigtextformat.util.Pair;

public class SortedLevelFile {
	private static final long MAX_CACHE_SIZE = 10000;
	private ExecutorService level0Exec = Executors.newFixedThreadPool(10,
			new ThreadFactory() {
				@Override
				public Thread newThread(Runnable r) {
					ThreadFactory tf = Executors.defaultThreadFactory();
					Thread t = tf.newThread(r);
					t.setName("Level0 Compact Writer for " + cwd.getPath());
					t.setDaemon(true);
					return t;
				}
			});

	private volatile List<MemtableSegment> segments = new ArrayList<>();

	private static class MemtableSegment {
		volatile Memtable current;
		List<Future<Void>> fut = new ArrayList<Future<Void>>();
		private ExecutorService exec = Executors.newFixedThreadPool(2,
				new ThreadFactory() {
					@Override
					public Thread newThread(Runnable r) {
						ThreadFactory tf = Executors.defaultThreadFactory();
						Thread t = tf.newThread(r);
						t.setName("Segment Writer");
						t.setDaemon(true);
						return t;
					}
				});

		public void setCurrent(Memtable current) {
			this.current = current;
		}

		public Memtable getCurrent() {
			return current;
		}

		public List<Future<Void>> getFut() {
			return fut;
		}

		public ExecutorService getExec() {
			return exec;
		}
	}

	FileLock fLock;

	ReadWriteLock lock = new ReentrantReadWriteLock();

	Levels levels = new Levels();

	private LevelOptions opts;

	List<LogFile> writing = new ArrayList<>();

	private File cwd;

	volatile boolean closed;

	private Compactor compactor;

	private int maxLevel = 0;

	volatile boolean compacting = false;

	private volatile Manifest manifest;

	private SortedLevelFile(File cwd, LevelOptions opt) throws Exception {
		this.opts = opt;
		this.cwd = cwd;
		this.manifest = new Manifest(cwd, this);
		Collection<LevelFile> levelFiles = manifest.getFiles();

		if (opts == null) {
			if (levelFiles.isEmpty())
				throw new EmptyDBException(cwd);
			try {
				LevelFile first = levelFiles.iterator().next();
				opts = first.getOpts();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		for (int i = 0; i < 4; i++) {
			MemtableSegment seg = new MemtableSegment();
			seg.setCurrent(new Memtable(cwd.getPath(), opts.format));
			this.segments.add(seg);

		}
		this.compactor = new Compactor(this, opts.maxCompactorThreads);
		for (LevelFile levelFile : levelFiles) {
			levelFile.setOpts(opts);
			addLevel(levelFile);
		}
	}

	private void recovery() throws Exception {
		new LevelRepairer(cwd.getPath()).repair(levels);

	}

	// private Set<LevelFile> checkIntersection(LevelFile levelFile,
	// List<LevelFile> files, int i) throws Exception {
	// Set<LevelFile> intersection = new HashSet<LevelFile>();
	// intersection.add(levelFile);
	// for (int j = i; j < files.size(); j++) {
	// LevelFile levelFile2 = files.get(j);
	// if (levelFile.intersectsWith(levelFile2))
	// intersection.add(levelFile2);
	// }
	// return intersection;
	// }

	public Compactor getCompactor() {
		return compactor;
	}

	protected void writeLevel0(Memtable table, boolean flush) throws Exception {
		Level level0 = getLevel(0);
		try {
			writeLevel0Data(table.getData(), level0);

			table.getLog().delete();

		} catch (Exception e1) {
			e1.printStackTrace();
		}

	}

	private void writeLevel0Data(TreeMap<byte[], byte[]> data, Level level0)
			throws Exception {
		CompactWriter writer = new CompactWriter(level0, level0Exec);

		for (Entry<byte[], byte[]> e : data.entrySet()) {
			writer.add(e.getKey(), e.getValue());
		}
		writer.persist();
	}

	public static String getMergedPath(String dir, int level, int cont) {
		return dir + "/" + level + "-" + cont + ".sst.merged";
	}

	public static String getPath(String dir, int level, int cont) {
		return dir + "/" + level + "-" + cont + ".sst";
	}

	public static String getTempPath(String dir, int level, int cont) {
		return dir + "/" + level + "-" + cont + ".sst.new";
	}

	public void addLevel(LevelFile level) throws Exception {
		Level list = getLevel(level.getLevel());
		list.add(level);
	}

	public Integer getLastLevelIndex(int i) {
		return getLevel(i).getLastLevelIndex();
	}

	public LevelOptions getOpts() {
		return opts;
	}

	AtomicInteger cont = new AtomicInteger(0);

	public void put(byte[] k, byte[] val) throws Exception {
		if (closed)
			throw new Exception("File closed");

		int index = (int) (cont.getAndIncrement() % segments.size());
		MemtableSegment seg = segments.get(index);
		synchronized (seg) {
			if (seg.getCurrent().logSize() >= opts.memTableSize)
				writeMemtable(false, seg);
			seg.getCurrent().put(k, val);
		}
	}

	private void writeMemtable(boolean flush, MemtableSegment seg)
			throws Exception {
		synchronized (seg) {
			final Memtable current = seg.getCurrent();
			if (current.isEmpty())
				return;
			seg.setCurrent(new Memtable(cwd.getPath(), opts.format));
			if (!opts.appendOnlyMode)
				awaitFutures(seg);

			scheduleMemtable(current, seg);

			if (flush) {
				awaitFutures(seg);
			}
		}

	}

	private void awaitFutures(MemtableSegment seg) throws InterruptedException,
			ExecutionException {
		Iterator<Future<Void>> it = seg.getFut().iterator();
		while (it.hasNext()) {
			Future<java.lang.Void> future = (Future<java.lang.Void>) it.next();
			future.get();
			it.remove();
		}
	}

	private void scheduleMemtable(final Memtable current, MemtableSegment seg) {
		seg.getFut().add(seg.getExec().submit(new Callable<Void>() {
			@Override
			public Void call() {
				try {
					current.closeLog();
					writeLevel0(current, false);

				} catch (IOException e) {
					e.printStackTrace();
				} catch (Exception e) {
					e.printStackTrace();
				}
				return null;

			}
		}));

	}

	public boolean exists(byte[] k) {
		return false;
	}

	public static SortedLevelFile open(String dir, final LevelOptions opts)
			throws Exception {

		SortedLevelFile ret = null;
		File fDir = new File(dir);
		if (!fDir.exists()) {
			fDir.mkdirs();
		}

		FileLock lock = FileChannel.open(Paths.get(dir + "/.lock"),
				StandardOpenOption.CREATE, StandardOpenOption.WRITE,
				StandardOpenOption.READ).tryLock();
		if (lock == null)
			throw new Exception("Database locked");
		File[] files = fDir.listFiles();
		List<File> memtables = new ArrayList<>();
		for (File file : files) {
			String name = file.getName();
			if (name.endsWith(".sst.new") || name.endsWith(".log.new"))
				file.delete();
			else if (name.endsWith(".LOG")) {
				Integer currentCount = Integer.valueOf(name.substring(0,
						name.indexOf(".")));

				Memtable.updateLogCount(currentCount);

				memtables.add(file);
			}

		}

		ret = new SortedLevelFile(fDir, opts);
		ret.fLock = lock;
		if (ret != null) {

			ret.recovery();
			ret.start();

			for (File path : memtables) {
				try {
					Memtable mem = Memtable.fromFile(path, opts.format);
					ret.scheduleMemtable(mem, ret.segments.get(0));
				} catch (Exception e) {
					path.delete();
					System.out.println(e.getMessage());
				}
			}
		}
		return ret;
	}

	private void start() {
		compactor.start();
	}

	public BlockFormat getFormat() {
		return opts.format;
	}

	public File getCwd() {
		return cwd;
	}

	public synchronized void close() throws Exception {
		for (MemtableSegment memtableSegment : segments) {
			memtableSegment.getExec().shutdown();
			memtableSegment.getExec().awaitTermination(Long.MAX_VALUE,
					TimeUnit.DAYS);
		}

		closed = true;

		compactor.waitFinished();

		for (MemtableSegment memtableSegment : segments) {
			writeMemtable(true, memtableSegment);
		}

		Level level0 = getLevel(0);
		if (level0 != null)
			synchronized (level0) {
				level0.notifyAll();
			}

		// flushWriting();
		for (Level list : levels) {
			for (LevelFile levelFile : list) {
				levelFile.close();
			}
		}

		levels.clear();

		manifest.close();

		fLock.release();
	}

	public int getMaxLevel() {
		lock.readLock().lock();
		int ret = maxLevel;
		lock.readLock().unlock();
		return ret;
	}

	public void createLevel(LevelFile from, int level) throws Exception {
		lock.writeLock().lock();
		try {
			Integer cont = getLevel(level).getLastLevelIndex();
			remove(from);
			from.moveTo(level, cont);
			addLevel(from);
		} finally {
			lock.writeLock().unlock();
		}
	}

	public void remove(LevelFile from) throws Exception {
		Level list = levels.get(from.getLevel());
		if (list != null)
			synchronized (list) {
				if (list != null) {
					list.remove(from);
					list.notifyAll();
				}
			}
	}

	public void delete(LevelFile levelFile) throws Exception {
		lock.writeLock().lock();
		try {
			levelFile.delete();
			remove(levelFile);
		} finally {
			lock.writeLock().unlock();
		}

	}

	public String print() throws Exception {
		lock.readLock().lock();
		try {
			StringBuilder builder = new StringBuilder();
			for (int i = 0; i <= getMaxLevel(); i++) {
				Level list = levels.get(i);
				if (list != null) {
					synchronized (list) {
						for (LevelFile levelFile : list) {
							builder.append(i + "-" + levelFile.print() + "\n");

						}
					}

				}
				builder.append("\n");
			}
			return builder.toString();
		} finally {
			lock.readLock().unlock();
		}

	}

	public void compact() throws Exception {
		synchronized (this) {
			this.compacting = true;

			for (MemtableSegment memtableSegment : segments) {
				writeMemtable(true, memtableSegment);
			}
			compactor.forcecompact();

			this.compacting = false;

			manifest.compact(levels.getMap());
		}
	}

	public boolean contains(byte[] k) throws Exception {
		synchronized (this) {
			if (segments.contains(k))
				return true;
		}

		for (int i = 0; i <= getMaxLevel(); i++) {
			Level list = getLevel(i);
			if (list.contains(k))
				return true;
		}

		return false;
	}

	public Level getLevel(int level) {
		Level list = levels.get(level);

		if (list == null) {
			lock.writeLock().lock();
			list = levels.get(level);
			if (list == null) {
				list = new Level(this, level);
				levels.put(level, list);
			}

			if (level > maxLevel)
				maxLevel = level;

			lock.writeLock().unlock();
		}

		return list;
	}

	AtomicInteger currentWriting = new AtomicInteger(0);

	public RangeIterator rangeIterator(byte[] from, byte[] to) throws Exception {
		return new RangeIterator(this, from, to);
	}

	public int compare(byte[] k1, byte[] k2) {
		return opts.format.compare(k1, k2);
	}

	public Pair<byte[], byte[]> getFirstInIntersection(byte[] from,
			boolean inclFrom, byte[] to, boolean inclTo) throws Exception {
		Pair<byte[], byte[]> min = null;

		for (MemtableSegment memtableSegment : segments) {
			synchronized (memtableSegment) {
				Pair<byte[], byte[]> inMemtable = memtableSegment.getCurrent()
						.getFirstIntersect(from, inclFrom, to, inclTo,
								opts.format);
				if (inMemtable != null
						&& (min == null || opts.format.compare(min.getKey(),
								inMemtable.getKey()) > 0))
					min = inMemtable;
			}
		}
		lock.readLock().lock();
		for (int i = 0; i <= getMaxLevel(); i++) {
			Level list = levels.get(i);
			if (list != null) {
				Pair<byte[], byte[]> inLevelFile = list.getFirstBetween(from,
						inclFrom, to, inclTo, opts.format);
				if (inLevelFile != null
						&& (min == null || opts.format.compare(min.getKey(),
								inLevelFile.getKey()) > 0)) {
					min = inLevelFile;
				}
			}
		}
		lock.readLock().unlock();

		return min;
	}

	@Override
	public String toString() {
		return cwd.getPath();
	}

	public byte[] get(byte[] k) {
		byte[] ret = null;
		for (MemtableSegment memtableSegment : segments) {
			synchronized (memtableSegment) {
				byte[] inMemtable = memtableSegment.getCurrent().get(k);
				if (inMemtable != null)
					return inMemtable;
			}
		}

		for (int i = 0; i <= getMaxLevel(); i++) {
			Level list = getLevel(i);
			ret = list.get(k);
			if (ret != null)
				return ret;
		}
		return ret;
	}

	public Manifest getManifest() {
		return manifest;
	}

	public void putIfNotExist(byte[] k, byte[] v) throws Exception {
		if (!contains(k))
			put(k, v);
	}
}