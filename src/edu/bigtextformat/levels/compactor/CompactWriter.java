package edu.bigtextformat.levels.compactor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import edu.bigtextformat.levels.Level;
import edu.bigtextformat.levels.datablock.DataBlock;
import edu.bigtextformat.levels.datablock.DataBlockWriter;
import edu.bigtextformat.levels.levelfile.LevelFile;

public class CompactWriter {

	private Level level;

	List<DataBlock> dbs = new ArrayList<>();

	int currSize = 0;

	private DataBlockWriter current = new DataBlockWriter();

	private Integer rate = null;

	private ExecutorService exec;

	byte[] minKey;

	AtomicInteger cont = new AtomicInteger();

	public CompactWriter(Level to, ExecutorService exec) {
		this.level = to;
		this.exec = exec;
	}

	public void add(byte[] k, byte[] v) throws Exception {
		check();
		current.add(k, v);
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

		if (currSize > level.getOpts().baseSize
				|| (level.level() > 0 && minKey != null && level.getFile()
						.getLevel(level.level() + 1)
						.intersectSize(minKey, maxKey) >= level.getOpts().intersectSplit))
			flushDBS();
	}

	public void addDataBlock(DataBlock dataBlock) throws Exception {
		flushCurrent();
		add(dataBlock);
	}

	private void check() throws Exception {
		if (current != null && current.size() >= level.getOpts().maxBlockSize) {
			flushCurrent();
		}
	}

	private void flushCurrent() throws Exception {
		if (current.size() > 0) {
			add(current.getDB());
			current.clear();
		}
	}

	private void flushDBS() throws InterruptedException {
		if (dbs.isEmpty())
			return;

		final List<DataBlock> currentList = dbs;

		dbs = new ArrayList<DataBlock>();
		currSize = 0;
		synchronized (cont) {
			while (cont.get() >= level.getOpts().maxCompactionWriters)
				cont.wait();
			cont.incrementAndGet();
		}

		exec.execute(new Runnable() {
			@Override
			public void run() {

				try {
					LevelFile curr = null;
					try {
						curr = LevelFile.newFile(level.getCwd().toString(),
								level.getOpts(), level.level(),
								level.getLastLevelIndex(), level.getFile());
					} catch (Exception e1) {
						e1.printStackTrace();
					}
					int size = 0;
					long time = System.currentTimeMillis();
					Iterator<DataBlock> it = currentList.iterator();
					while (it.hasNext()) {
						DataBlock dataBlock = (DataBlock) it.next();
						try {
							curr.put(dataBlock);
							if (rate != null) {
								size += dataBlock.size();
								long diff = (System.currentTimeMillis() - time);
								if (size / ((float) diff) > rate) { // 10MB per
									// sec
									long l = (long) ((size / (float) rate) - diff);
									// System.out.println("Current rate exceeded "
									// + (size / ((float) diff)) + " waiting " +
									// l +
									// " ms");
									Thread.sleep(l);
									size = 0;
									time = System.currentTimeMillis();
								}
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
						it.remove();
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
				} finally {
					synchronized (cont) {
						cont.decrementAndGet();
						cont.notify();
					}
				}
			}
		});
	}

	public void persist() throws Exception {
		flushCurrent();
		flushDBS();

		synchronized (cont) {
			while (cont.get() != 0)
				cont.wait();
		}

	}

	public void setTrottle(int rate) {
		this.rate = rate;
	}
}