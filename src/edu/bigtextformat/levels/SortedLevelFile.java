package edu.bigtextformat.levels;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.levels.compactor.CompactWriterV3;
import edu.bigtextformat.levels.compactor.Compactor;
import edu.bigtextformat.levels.compactor.CompactorV2;
import edu.bigtextformat.levels.compactor.LevelMerger;
import edu.bigtextformat.levels.compactor.Writer;
import edu.bigtextformat.levels.levelfile.LevelFile;

public class SortedLevelFile {

	ExecutorService exec = Executors.newFixedThreadPool(10);

	Future<Void> fut = null;

	private volatile Memtable memTable;

	ReadWriteLock lock = new ReentrantReadWriteLock();

	Levels levels = new Levels();

	private LevelOptions opts;

	List<LogFile> writing = new ArrayList<>();

	private File cwd;

	volatile boolean closed;

	private LogFileWriter writer;

	private Compactor compactor;

	private int maxLevel = 0;

	volatile boolean compacting = false;

	private volatile Manifest manifest;

	private SortedLevelFile(File cwd, LevelOptions opt) throws Exception {
		this.opts = opt;
		this.cwd = cwd;
		this.manifest = new Manifest(cwd);
		Collection<LevelFile> levelFiles = manifest.readLevelFiles();

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
		this.memTable = new Memtable(cwd.getPath(), opts.format);
		this.writer = new LogFileWriter(this);
		this.compactor = new CompactorV2(this, opts.maxCompactorThreads);
		for (LevelFile levelFile : levelFiles) {
			addLevel(levelFile);
		}
	}

	private void recovery() throws Exception {
		boolean clean = false;
		while (!clean) {
			clean = true;
			for (final Level to : levels) {
				if (to.level() > 0) {
					HashSet<String> currentlyMerging = new HashSet<String>();
					ExecutorService execRec = Executors.newFixedThreadPool(20);
					for (LevelFile lf : to) {
						if (!lf.isDeleted()
								&& !currentlyMerging.contains(lf.getName())) {
							final Set<LevelFile> i = to.intersect(
									lf.getMinKey(), lf.getMaxKey());
							if (i.size() > 1) {
								boolean alreadyMerging = false;
								for (LevelFile levelFile : i) {
									if (currentlyMerging.contains(levelFile
											.getName()))
										alreadyMerging = true;
								}
								if (!alreadyMerging) {
									for (LevelFile levelFile : i) {
										currentlyMerging.add(levelFile
												.getName());
									}

									execRec.execute(new Runnable() {

										@Override
										public void run() {
											System.out
													.println("Recovering failed merge among "
															+ i.size()
															+ " files. ");
											// for (LevelFile levelFile : i) {
											// System.out.println("p:"
											// + levelFile.getPath()
											// + " n:"
											// + levelFile.getName());
											// }
											try {
												LevelMerger.shrink(to, i);
											} catch (Exception e) {
												e.printStackTrace();
											}

										}
									});
								}
								clean = false;
							}
						}
					}
					execRec.shutdown();
					execRec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
				}
			}
		}
	}

	public Compactor getCompactor() {
		return compactor;
	}

	protected void writeLevel0(Memtable table, boolean flush) throws Exception {
		// if (table.size() == 0)
		// return;
		Level level0 = getLevel(0);
		// synchronized (level0) {
		// while (level0.size() >= opts.maxLevel0Files && !flush && !closed)
		// try {
		// compactor.compact(0);
		// level0.wait();
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// }
		// }
		// Integer cont = getLastLevelIndex(0);

		// TreeMap<byte[], byte[]> map = new TreeMap<>(opts.format);

		try {
			// for (byte[] data : table) {
			// Operation op = new Operation().fromByteArray(data);
			// if (op.op.equals(Operations.PUT))
			// map.put(op.k, op.v);
			// }
			writeLevel0Data(table.data, level0);

			table.getLog().delete();

		} catch (Exception e1) {
			e1.printStackTrace();
			// table.close();
			// table.delete();
		}

	}

	private void writeLevel0Data(TreeMap<byte[], byte[]> data, Level level0)
			throws Exception {
		Writer writer = null;
		if (opts.splitMemtable)
			writer = new CompactWriterV3(level0);
		else
			writer = new SingleFileWriter(level0);

		for (Entry<byte[], byte[]> e : data.entrySet()) {
			writer.add(e.getKey(), e.getValue());
		}
		writer.persist();
	}

	// protected void writeLevel0(LogFile table, boolean flush) throws Exception
	// {
	// // if (table.size() == 0)
	// // return;
	// Level level0 = getLevel(0);
	// synchronized (level0) {
	// while (level0.size() >= opts.maxLevel0Files && !flush && !closed)
	// try {
	// compactor.compact(0);
	// level0.wait();
	// } catch (InterruptedException e) {
	// e.printStackTrace();
	// }
	// }
	// // Integer cont = getLastLevelIndex(0);
	// Writer writer = null;
	// if (opts.splitMemtable)
	// writer = new CompactWriterV3(level0);
	// else
	// writer = new SingleFileWriter(level0);
	//
	// TreeMap<byte[], byte[]> map = new TreeMap<>(opts.format);
	//
	// try {
	// for (byte[] data : table) {
	// Operation op = new Operation().fromByteArray(data);
	// if (op.op.equals(Operations.PUT))
	// map.put(op.k, op.v);
	// }
	//
	// for (Entry<byte[], byte[]> e : map.entrySet()) {
	// writer.add(e.getKey(), e.getValue());
	// }
	// writer.persist();
	//
	// table.delete();
	//
	// } catch (Exception e1) {
	// e1.printStackTrace();
	// table.close();
	// table.delete();
	// }
	//
	// }

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
			if (memTable.logSize() >= opts.memTableSize)
				writeMemtable(false);
			memTable.put(k, val);
		}
	}

	private synchronized void writeMemtable(boolean flush) throws Exception {
		if (memTable.isEmpty())
			return;

		final Memtable current = memTable;

		if (opts.appendOnlyMode) {
			exec.execute(new Runnable() {
				@Override
				public void run() {
					try {
						current.closeLog();
						writeLevel0(current, false);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}
			});
		} else {
			if (fut != null)
				fut.get();
			fut = exec.submit(new Callable<Void>() {
				@Override
				public Void call() {
					try {
						current.closeLog();
						writeLevel0(current, false);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					return null;

				}
			});
		}

		memTable = new Memtable(cwd.getPath(), opts.format);
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
		if (ret != null) {

			ret.recovery();
			ret.start();

			if (!opts.appendOnlyMode)
				for (File path : memtables) {
					// ret.writing.add();
					TreeMap<byte[], byte[]> map = new TreeMap<>(opts.format);
					LogFile table = new LogFile(path);
					try {
						for (byte[] data : table) {
							Operation op = new Operation().fromByteArray(data);
							if (op.op.equals(Operations.PUT))
								map.put(op.k, op.v);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}

					ret.writeLevel0Data(map, ret.getLevel(0));
					table.delete();
				}
			// ret.flushWriting();
		}
		return ret;
	}

	private void start() {
		writer.start();
		compactor.start();
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

		// flushWriting();
		for (Level list : levels) {
			for (LevelFile levelFile : list) {
				levelFile.close();
			}
		}
		levels.clear();
		manifest.close();

		exec.shutdown();
		exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
	}

	public int getMaxLevel() {
		lock.readLock().lock();
		int ret = maxLevel;
		lock.readLock().unlock();
		return ret;

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
							builder.append(i + "-"
									+ levelFile.print(opts.format) + "\n");

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
			// System.out.println("Writing memtable");

			writeMemtable(true);

			// flushWriting();
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

			manifest.compact();
		}
	}

	// private void flushWriting() throws Exception {
	// // System.out.println("Writing level0");
	// while (writeNextMemtable())
	// ;
	// }

	public boolean contains(byte[] k) throws Exception {
		synchronized (this) {
			if (memTable.contains(k))
				return true;
		}

		synchronized (writing) {
			for (LogFile log : writing) {
				for (byte[] bs : log) {
					Operation op = new Operation().fromByteArray(bs);
					if (op.op.equals(Operations.PUT) && Arrays.equals(op.k, k))
						return true;
				}
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

	// public boolean writeNextMemtable() throws Exception {
	// LogFile table = null;
	// synchronized (writing) {
	// int i = currentWriting.get();
	// if (i >= writing.size())
	// return false;
	// table = writing.get(i);
	// currentWriting.incrementAndGet();
	// }
	//
	// table.close();
	//
	// writeLevel0(table, false);
	//
	// synchronized (writing) {
	// writing.remove(table);
	// writing.notifyAll();
	// currentWriting.decrementAndGet();
	// }
	// return true;
	// }

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
		// synchronized (writing) {
		// for (LogFile dataBlock : writing) {
		// Pair<byte[], byte[]> inWriting = dataBlock.getFirstIntersect(
		// from, inclFrom, to, inclTo, opts.format);
		// if (min == null
		// || opts.format
		// .compare(min.getKey(), inWriting.getKey()) > 0) {
		// min = inWriting;
		// }
		// }
		// }

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
		synchronized (this) {
			ret = memTable.get(k);
			if (ret != null)
				return ret;
		}
		synchronized (writing) {
			// for (LogFile dataBlock : writing) {
			// ret = dataBlock.get(k);
			// if (ret != null)
			// return ret;
			// }
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

	public Manifest getManifest() {
		return manifest;
	}

	public void putIfNotExist(byte[] k, byte[] v) throws Exception {
		if (!contains(k))
			put(k, v);
	}
}