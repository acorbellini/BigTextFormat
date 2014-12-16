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
import edu.bigtextformat.levels.memtable.MemtableSegment;
import edu.bigtextformat.manifest.Manifest;
import edu.bigtextformat.util.LogFile;
import edu.bigtextformat.util.Pair;

public class SortedLevelFile {
	public static String getMergedPath(String dir, int level, int cont) {
		return dir + "/" + level + "-" + cont + ".sst.merged";
	}

	public static String getPath(String dir, int level, int cont) {
		return dir + "/" + level + "-" + cont + ".sst";
	}

	public static String getTempPath(String dir, int level, int cont) {
		return dir + "/" + level + "-" + cont + ".sst.new";
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
					Memtable mem = Memtable.fromFile(path, opts);
					ret.scheduleMemtable(mem, ret.segments.get(0));
				} catch (Exception e) {
					// path.delete();
					System.out.println(e.getMessage());
				}
			}
		}
		return ret;
	}

	private ExecutorService level0Exec;

	private volatile List<MemtableSegment> segments = new ArrayList<>();

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

	AtomicInteger cont = new AtomicInteger(0);

	AtomicInteger currentWriting = new AtomicInteger(0);

	private SortedLevelFile(final File cwd, LevelOptions opt) throws Exception {
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

		level0Exec = Executors.newFixedThreadPool(opts.maxLevel0WriterThreads,
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

		for (int i = 0; i < opts.maxMemtableSegments; i++) {
			MemtableSegment seg = new MemtableSegment(opts.maxSegmentWriters);
			seg.setCurrent(new Memtable(cwd.getPath(), opts));
			this.segments.add(seg);

		}
		this.compactor = new Compactor(this);
		for (LevelFile levelFile : levelFiles) {
			levelFile.setOpts(opts);
			addLevel(levelFile);
		}
	}

	public void addLevel(LevelFile level) throws Exception {
		Level list = getLevel(level.getLevel());
		list.add(level);
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

	public int compare(byte[] k1, byte[] k2) {
		return opts.format.compare(k1, k2);
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

	public boolean exists(byte[] k) {
		return false;
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

	public Compactor getCompactor() {
		return compactor;
	}

	public File getCwd() {
		return cwd;
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

	public BlockFormat getFormat() {
		return opts.format;
	}

	public Integer getLastLevelIndex(int i) {
		return getLevel(i).getLastLevelIndex();
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

	public Manifest getManifest() {
		return manifest;
	}

	public int getMaxLevel() {
		lock.readLock().lock();
		int ret = maxLevel;
		lock.readLock().unlock();
		return ret;
	}

	public LevelOptions getOpts() {
		return opts;
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

	public void putIfNotExist(byte[] k, byte[] v) throws Exception {
		if (!contains(k))
			put(k, v);
	}

	public RangeIterator rangeIterator(byte[] from, byte[] to) throws Exception {
		return new RangeIterator(this, from, to);
	}

	private void recovery() throws Exception {
		new LevelRepairer(this).repair(levels);

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

	private void start() {
		compactor.start();
	}

	@Override
	public String toString() {
		return cwd.getPath();
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

	private void writeMemtable(boolean flush, MemtableSegment seg)
			throws Exception {
		synchronized (seg) {
			final Memtable current = seg.getCurrent();
			if (current.isEmpty())
				return;
			seg.setCurrent(new Memtable(cwd.getPath(), opts));

			awaitFutures(seg);

			scheduleMemtable(current, seg);

			if (flush) {
				awaitFutures(seg);
			}
		}

	}
}