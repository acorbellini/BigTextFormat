package edu.bigtextformat.levels;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.levels.levelfile.LevelFile;
import edu.bigtextformat.levels.levelfile.LevelFileWriter;

public class SortedLevelFile {
	AtomicInteger count = new AtomicInteger(0);
	private volatile DataBlock memTable;
	HashMap<Integer, List<LevelFile>> levels = new HashMap<>();
	private LevelOptions opts;
	List<DataBlock> writing = new ArrayList<>();
	private File cwd;
	volatile boolean closed;
	private Level0Writer writer;
	private Compactor compactor;
	private int maxLevel = 0;
	volatile boolean compacting = false;

	private SortedLevelFile(File cwd, LevelOptions opt) {
		this.opts = opt;
		this.cwd = cwd;
		memTable = new DataBlock();
		writer = new Level0Writer(this);
		compactor = new Compactor(this);
	}

	protected void writeLevel0(DataBlock table, boolean flush) throws Exception {
		List<LevelFile> level0 = levels.get(0);
		if (level0 != null) {
			synchronized (level0) {
				while (level0.size() >= opts.maxLevel0Files && !flush)
					try {
						level0.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
			}
		}
		Integer cont = getLastLevelIndex(0);
		LevelFile level = LevelFile.newFile(getCwd().getPath(), opts, 0, cont);
		LevelFileWriter levelwriter = level.getWriter();
		List<byte[]> keys = table.getKeys();
		List<byte[]> values = table.getValues();
		for (int i = 0; i < keys.size(); i++)
			levelwriter.add(keys.get(i), values.get(i));
		levelwriter.close();
		level.commitAndPersist();
		addLevel(level);
	}

	public static String getPath(String dir, int level, int cont) {
		return dir + "/" + level + "-" + cont + ".sst";
	}

	public static String getTempPath(String dir, int level, int cont) {
		return dir + "/" + level + "-" + cont + ".sst.new";
	}

	void addLevel(LevelFile level) {
		List<LevelFile> list = levels.get(level.getLevel());
		synchronized (levels) {
			if (list == null) {
				list = levels.get(level.getLevel());
				if (list == null) {
					list = new ArrayList<>();
					levels.put(level.getLevel(), list);
				}
			}

			if (level.getLevel() > maxLevel)
				maxLevel = level.getLevel();
		}
		synchronized (list) {
			list.add(level);
			if (level.getLevel() == 0)
				list.notify();
		}

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
			memTable.insertOrdered(k, val, opts.format);
		}
	}

	private synchronized void writeMemtable(boolean flush) {
		if (memTable.size() == 0)
			return;
		synchronized (writing) {
			writing.add(memTable);
			writing.notify();
			while (writing.size() >= opts.maxMemTablesWriting && !flush)
				try {
					writing.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
		}
		memTable = new DataBlock();
	}

	public boolean exists(byte[] k) {
		return false;
	}

	public static SortedLevelFile open(String dir, LevelOptions opts) {
		SortedLevelFile ret = null;
		File fDir = new File(dir);
		if (!fDir.exists()) {
			fDir.mkdirs();
		}
		if (fDir.list().length == 0) {
			ret = createNew(fDir, opts);
		}
		if (ret != null)
			ret.start();
		return ret;
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

		writeMemtable(true);

		flushWriting();

		compactor.waitFinished();

		for (Entry<Integer, List<LevelFile>> l : levels.entrySet()) {
			List<LevelFile> list = l.getValue();
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

	public List<LevelFile> getLevelFile(byte[] minKey, byte[] maxKey, int l) {
		List<LevelFile> found = new ArrayList<>();
		List<LevelFile> list = levels.get(l);
		if (list != null) {
			for (LevelFile levelFile : list) {
				if (opts.format.compare(levelFile.getMinKey(), maxKey) > 0
						|| opts.format.compare(levelFile.getMaxKey(), minKey) < 0)
					;
				else
					found.add(levelFile);
			}
		}
		return found;
	}

	public void moveTo(LevelFile from, int level) throws Exception {
		synchronized (levels) {
			Integer cont = getLastLevelIndex(level);
			remove(from);
			from.moveTo(level, cont);
			addLevel(from);
		}

	}

	public void remove(LevelFile from) {
		List<LevelFile> list = levels.get(from.getLevel());
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
				List<LevelFile> list = levels.get(i);
				if (list != null) {
					for (LevelFile levelFile : list) {
						builder.append(levelFile.print(opts.format));

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
			System.out.println("Writing memtable");

			writeMemtable(true);

			flushWriting();
			//
			// List<LevelFile> level0 = getLevel(0);
			// synchronized (level0) {
			// level0.notifyAll();
			// while (!level0.isEmpty())
			// level0.wait();
			// }
			System.out.println("Running compactor");
			while (compactor.check(true)) {
				List<LevelFile> level0 = getLevel(0);
				synchronized (level0) {
					level0.notifyAll();
				}
			}
			this.compacting = true;
		}
	}

	private void flushWriting() throws Exception {
		System.out.println("Writing level0");
		synchronized (writing) {
			for (DataBlock dataBlock : writing) {
				writeLevel0(dataBlock, true);
			}
			writing.notifyAll();
		}
	}

	public boolean contains(byte[] k) throws Exception {
		synchronized (this) {
			if (memTable.contains(k, opts.format))
				return true;
		}
		synchronized (writing) {
			for (DataBlock dataBlock : writing) {
				if (dataBlock.contains(k, opts.format))
					return true;
			}
		}

		synchronized (levels) {
			for (int i = 0; i <= getMaxLevel(); i++) {
				List<LevelFile> list = levels.get(i);
				if (list != null) {
					for (LevelFile levelFile : list) {
						if (levelFile.contains(k, opts.format))
							return true;
					}
				}
			}
		}

		return false;
	}

	public List<LevelFile> getLevel(int i) {
		synchronized (levels) {
			return levels.get(i);
		}
	}

	public DataBlock getNextMemtable() {
		DataBlock table = null;
		synchronized (writing) {
			while (writing.isEmpty() && !closed)
				try {
					writing.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			if (closed)
				return null;
			if (!writing.isEmpty())
				table = writing.get(0);
		}
		return table;
	}

	public void removeMemtable(DataBlock table) {
		synchronized (writing) {
			writing.remove(table);
			writing.notifyAll();
		}
	}

}