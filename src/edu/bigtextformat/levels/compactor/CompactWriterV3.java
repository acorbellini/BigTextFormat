package edu.bigtextformat.levels.compactor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import edu.bigtextformat.levels.DataBlock;
import edu.bigtextformat.levels.DataBlockWriter;
import edu.bigtextformat.levels.Level;
import edu.bigtextformat.levels.levelfile.LevelFile;

public class CompactWriterV3 implements Writer {
	// private static final int RATE = 2 * 1024 * 1024 / 1000;
	private Level level;
	List<DataBlock> dbs = new ArrayList<>();
	List<Future<Void>> futures = new ArrayList<>();
	int currSize = 0;
	private DataBlockWriter current = new DataBlockWriter();
	private static ExecutorService execShared = Executors.newFixedThreadPool(4,
			new ThreadFactory() {
				@Override
				public Thread newThread(Runnable r) {
					ThreadFactory tf = Executors.defaultThreadFactory();
					Thread t = tf.newThread(r);
					t.setName("Compact Writer Thread");
					t.setDaemon(true);
					return t;
				}
			});
	boolean trottle = false;
	private int rate;
	private ExecutorService exec;
	byte[] minKey;

	public CompactWriterV3(Level to) {
		this(to, execShared);
	}

	public CompactWriterV3(Level to, ExecutorService execShared2) {
		this.level = to;
		this.exec = execShared2;
	}

	public void setTrottle(int rate) {
		this.trottle = true;
		this.rate = rate;
	}

	public void addDataBlock(DataBlock dataBlock) throws Exception {
		flushCurrent();
		add(dataBlock);
	}

	private void add(DataBlock dataBlock) throws Exception {
		dbs.add(dataBlock);

		try {
			if (dbs.size() == 1)
				minKey = dbs.get(0).firstKey();
		} catch (Exception e) {
			dbs.remove(dataBlock);
			throw e;
		}

		currSize += dataBlock.size();

		byte[] maxKey = dataBlock.lastKey();

		float min = Math.min(level.getOpts().maxSize,
				((level.level() / (float) level.getOpts().sizeModifier) + 1)
						* level.getOpts().baseSize);
		if (currSize > min
				|| (level.level() > 0 && minKey != null && level.getFile()
						.getLevel(level.level() + 1)
						.intersectSize(minKey, maxKey) >= level.getOpts().intersectSplit)) {
			flushDBS();
		}
	}

	private void flushDBS() {
		if (dbs.isEmpty())
			return;
		final List<DataBlock> currentList = dbs;
		dbs = new ArrayList<DataBlock>();
		currSize = 0;
		futures.add(exec.submit(new Callable<Void>() {
			@Override
			public Void call() {

				LevelFile curr = null;
				try {
					curr = LevelFile.newFile(level.getCwd().toString(),
							level.getOpts(), level.level(),
							level.getLastLevelIndex());
				} catch (Exception e1) {
					e1.printStackTrace();
				}
				int size = 0;
				long time = System.currentTimeMillis();
				for (DataBlock dataBlock2 : currentList) {
					try {
						curr.put(dataBlock2);
						if (trottle) {
							size += dataBlock2.size();
							long diff = (System.currentTimeMillis() - time);
							if (size / ((float) diff) > rate) { // 10MB per sec
								long l = (long) ((size / (float) rate) - diff);
								// System.out.println("Current rate exceeded "
								// + (size / ((float) diff)) + " waiting " + l +
								// " ms");
								Thread.sleep(l);
								size = 0;
								time = System.currentTimeMillis();
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
					}

				}

				try {
					curr.commitAndPersist();
					curr.close();

				} catch (Exception e) {
					e.printStackTrace();
				}
				try {
					level.add(curr);
				} catch (Exception e) {
					e.printStackTrace();
				}
				return null;
			}
		}));
	}

	private void flushCurrent() throws Exception {
		if (current.size() > 0) {
			add(current.getDB());
			current.clear();
		}
	}

	public void persist() throws Exception {
		flushCurrent();
		flushDBS();

		// exec.shutdown();
		// exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

		for (Future<Void> future : futures) {
			future.get();
		}

	}

	public void add(byte[] k, byte[] v) throws Exception {
		check();
		current.add(k, v);
	}

	private void check() throws Exception {
		if (current != null && current.size() >= level.getOpts().maxBlockSize) {
			flushCurrent();
		}
	}
}