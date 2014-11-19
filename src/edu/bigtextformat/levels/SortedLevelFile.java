package edu.bigtextformat.levels;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.levels.compactor.CompactWriterV2;
import edu.bigtextformat.levels.compactor.CompactorInterface;
import edu.bigtextformat.levels.compactor.CompactorV2;
import edu.bigtextformat.levels.levelfile.LevelFile;

public class SortedLevelFile {
	AtomicInteger count = new AtomicInteger(0);
	private volatile Memtable memTable;
	Map<Integer, Level> levels = new ConcurrentHashMap<>();
	private LevelOptions opts;
	List<Memtable> writing = new ArrayList<>();
	private File cwd;
	volatile boolean closed;
	private Level0Writer writer;
	private CompactorInterface compactor;
	private int maxLevel = 0;
	volatile boolean compacting = false;

	private SortedLevelFile(File cwd, LevelOptions opt) {
		this.opts = opt;
		this.cwd = cwd;
		memTable = new Memtable(opt.format);
		writer = new Level0Writer(this);
		compactor = new CompactorV2(this, opts.maxCompactorThreads);
	}

	public CompactorInterface getCompactor() {
		return compactor;
	}

	protected void writeLevel0(Memtable dataBlock, boolean flush)
			throws Exception {
		if (dataBlock.size() == 0)
			return;
		Level level0 = getLevel(0);
		synchronized (level0) {
			while (level0.size() >= opts.maxLevel0Files && !flush && !closed)
				try {
					level0.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
		}
		Integer cont = getLastLevelIndex(0);
		// LevelFile level = LevelFile.newFile(getCwd().getPath(), opts, 0,
		// cont);
		// LevelFileWriter lwriter = level.getWriter();
		CompactWriterV2 lwriter = new CompactWriterV2(level0);
		// List<byte[]> keys = dataBlock.getKeys();
		// List<byte[]> values = dataBlock.getValues();
		// for (int i = 0; i < keys.size(); i++)
		// lwriter.add(keys.get(i), values.get(i));
		for (Entry<byte[], byte[]> e : dataBlock.data.entrySet()) {
			lwriter.add(e.getKey(), e.getValue());
		}

		// lwriter.close();
		lwriter.persist();
		// level.commitAndPersist();
		// level0.add(level);
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
		return count.incrementAndGet();
		// while (true) {
		// int curr = count.incrementAndGet();
		// if (!Files.exists(Paths.get(getCwd() + "/" + i + "-" + curr
		// + ".sst")))
		// return curr;
		// }
	}

	public LevelOptions getOpts() {
		return opts;
	}

	public void put(byte[] k, byte[] val) throws Exception {
		if (closed)
			throw new Exception("File closed");
		synchronized (this) {
			if (memTable.size() >= opts.memTableSize)
				writeMemtable(false);
			memTable.insertOrdered(k, val);
		}
	}

	private synchronized void writeMemtable(boolean flush) {
		if (memTable.size() == 0)
			return;
		synchronized (writing) {
			writing.add(memTable);
			writer.setChanged();
			while (writing.size() >= opts.maxMemTablesWriting && !flush)
				try {
					writing.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
		}
		memTable = new Memtable(opts.format);
	}

	public boolean exists(byte[] k) {
		return false;
	}

	public static SortedLevelFile open(String dir, LevelOptions opts)
			throws Exception {
		SortedLevelFile ret = null;
		File fDir = new File(dir);
		if (!fDir.exists()) {
			fDir.mkdirs();
		}

		File[] files = fDir.listFiles();
		for (File file : files) {
			if (file.getPath().endsWith(".sst.new"))
				file.delete();
		}

		if (fDir.list().length == 0) {
			ret = createNew(fDir, opts);
		} else
			ret = openInternal(fDir, opts);
		if (ret != null)
			ret.start();
		return ret;
	}

	private static SortedLevelFile openInternal(File fDir, LevelOptions opts)
			throws Exception {
		ExecutorService exec = Executors.newFixedThreadPool(20);
		final Semaphore sem = new Semaphore(20);
		final List<LevelFile> levelFiles = new ArrayList<>();
		File[] files = fDir.listFiles();
		final int[] maxCount = new int[] { 0 };
		for (final File file : files) {
			sem.acquire();
			exec.execute(new Runnable() {
				@Override
				public void run() {
					try {
						if (file.getPath().endsWith(".sst.new"))
							file.delete();
						if (file.getPath().endsWith(".sst")) {
							// System.out.println("Opening " + file);
							LevelFile open = LevelFile.open(file);
							// System.out.println("Opened " + file);
							synchronized (levelFiles) {
								levelFiles.add(open);
								if (maxCount[0] < open.getCont())
									maxCount[0] = open.getCont();
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						sem.release();
					}
				}
			});
		}
		exec.shutdown();
		exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

		LevelOptions lo = opts;
		if (lo == null)
			lo = new LevelOptions().fromByteArray(levelFiles.get(0).getFile()
					.getHeader().get("opts"));

		SortedLevelFile f = new SortedLevelFile(fDir, lo);
		f.count = new AtomicInteger(maxCount[0]);
		for (LevelFile levelFile : levelFiles) {
			f.addLevel(levelFile);
		}

		return f;
	}

	private void start() {
		writer.start();
		compactor.start();

	}

	private static SortedLevelFile createNew(File fDir, LevelOptions opts) {
		return new SortedLevelFile(fDir, opts);
	}

	public BlockFormat getFormat() {
		return opts.format;
	}

	public File getCwd() {
		return cwd;
	}

	public synchronized void close() throws Exception {
		closed = true;
		writer.waitFinished();
		compactor.waitFinished();

		writeMemtable(true);

		Level level0 = getLevel(0);
		if (level0 != null)
			synchronized (level0) {
				level0.notifyAll();
			}

		flushWriting();
		for (Entry<Integer, Level> l : levels.entrySet()) {
			Level list = l.getValue();
			for (LevelFile levelFile : list) {
				levelFile.close();
			}
		}
		levels.clear();

	}

	public int getMaxLevel() {
		synchronized (levels) {
			return maxLevel;
		}

		// int max = 0;
		// Set<Entry<Integer, List<LevelFile>>> it = levels.entrySet();
		// for (Entry<Integer, List<LevelFile>> entry : it) {
		// if (entry.getKey() > max)
		// max = entry.getKey();
		// }
		// return max;
	}

	// public Set<LevelFile> intersect(byte[] minKey, byte[] maxKey, int l) {
	// Set<LevelFile> found = new HashSet<>();
	// Level list = null;
	// synchronized (levels) {
	// list = levels.get(l);
	// }
	// if (list != null) {
	// synchronized (list) {
	// for (LevelFile levelFile : list) {
	// if (opts.format.compare(levelFile.getMinKey(), maxKey) > 0
	// || opts.format.compare(levelFile.getMaxKey(),
	// minKey) < 0)
	// ;
	// else
	// found.add(levelFile);
	// }
	// }
	// }
	// return found;
	// }

	public void createLevel(LevelFile from, int level) throws Exception {
		synchronized (levels) {
			Integer cont = getLastLevelIndex(level);
			remove(from);
			from.moveTo(level, cont);
			addLevel(from);
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
		synchronized (levels) {
			levelFile.delete();
			remove(levelFile);
		}
	}

	public String print() throws Exception {
		synchronized (levels) {
			StringBuilder builder = new StringBuilder();
			for (int i = 0; i <= getMaxLevel(); i++) {
				builder.append(i + "-");
				Level list = levels.get(i);
				if (list != null) {
					synchronized (list) {
						for (LevelFile levelFile : list) {
							builder.append(levelFile.print(opts.format));

						}
					}

				}
				builder.append("\n");
			}
			return builder.toString();
		}
	}

	public void compact() throws Exception {
		synchronized (this) {
			this.compacting = true;
			// System.out.println("Writing memtable");

			writeMemtable(true);

			flushWriting();
			//
			// List<LevelFile> level0 = getLevel(0);
			// synchronized (level0) {
			// level0.notifyAll();
			// while (!level0.isEmpty())
			// level0.wait();
			// }
			// System.out.println("Running compactor");
			compactor.forcecompact();

			this.compacting = false;
		}
	}

	private void flushWriting() throws Exception {
		// System.out.println("Writing level0");
		while (writeNextMemtable())
			;
	}

	public boolean contains(byte[] k) throws Exception {
		synchronized (this) {
			if (memTable.contains(k))
				return true;
		}
		synchronized (writing) {
			for (Memtable dataBlock : writing) {
				if (dataBlock.contains(k))
					return true;
			}
		}

		// synchronized (levels) {
		for (int i = 0; i <= getMaxLevel(); i++) {
			Level list = getLevel(i);
			if (list.contains(k))
				return true;
		}
		// }

		return false;
	}

	public Level getLevel(int level) {
		Level list = levels.get(level);
		if (list == null)
			synchronized (levels) {
				if (list == null) {
					list = levels.get(level);
					if (list == null) {
						list = new Level(this, level);
						levels.put(level, list);
					}
				}

				if (level > maxLevel)
					maxLevel = level;
			}
		return list;
	}

	AtomicInteger currentWriting = new AtomicInteger(0);

	public boolean writeNextMemtable() throws Exception {
		Memtable table = null;
		synchronized (writing) {
			int i = currentWriting.get();
			if (i >= writing.size())
				return false;
			table = writing.get(i);
			currentWriting.incrementAndGet();
		}

		writeLevel0(table, false);

		synchronized (writing) {
			writing.remove(table);
			writing.notifyAll();
			currentWriting.decrementAndGet();
		}
		return true;
	}

	public RangeIterator rangeIterator(byte[] from, byte[] to) throws Exception {
		return new RangeIterator(this, from, to);
	}

	public int compare(byte[] k1, byte[] k2) {
		return opts.format.compare(k1, k2);
	}

	public Pair<byte[], byte[]> getFirstInIntersection(byte[] from,
			boolean inclFrom, byte[] to, boolean inclTo) throws Exception {
		Pair<byte[], byte[]> min = null;
		synchronized (this) {
			min = memTable.getFirstIntersect(from, inclFrom, to, inclTo,
					opts.format);
		}
		synchronized (writing) {
			for (Memtable dataBlock : writing) {
				Pair<byte[], byte[]> inWriting = dataBlock.getFirstIntersect(
						from, inclFrom, to, inclTo, opts.format);
				if (min == null
						|| opts.format
								.compare(min.getKey(), inWriting.getKey()) > 0) {
					min = inWriting;
				}
			}
		}

		synchronized (levels) {
			for (int i = 0; i <= getMaxLevel(); i++) {
				Level list = levels.get(i);
				if (list != null) {
					Iterator<LevelFile> it = list.iterator();
					boolean found = false;
					while (it.hasNext() && !found) {
						LevelFile levelFile = it.next();
						Pair<byte[], byte[]> inLevelFile = levelFile
								.getFirstBetween(from, inclFrom, to, inclTo,
										opts.format);
						if (inLevelFile != null
								&& (min == null || opts.format.compare(
										min.getKey(), inLevelFile.getKey()) > 0)) {
							min = inLevelFile;
							found = true;
						}
					}
				}
			}
		}

		return min;
	}

	@Override
	public String toString() {
		return cwd.getPath();
	}

	public byte[] get(byte[] k) {
		byte[] ret = null;
		synchronized (this) {
			ret = memTable.get(k);
			if (ret != null)
				return ret;
		}
		synchronized (writing) {
			for (Memtable dataBlock : writing) {
				ret = dataBlock.get(k);
				if (ret != null)
					return ret;
			}
		}

		// synchronized (levels) {
		for (int i = 0; i <= getMaxLevel(); i++) {
			Level list = getLevel(i);
			ret = list.get(k);
			if (ret != null)
				return ret;
		}
		return ret;
	}
}