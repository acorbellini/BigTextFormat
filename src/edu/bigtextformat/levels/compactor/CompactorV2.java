package edu.bigtextformat.levels.compactor;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import edu.bigtextformat.levels.SortedLevelFile;

public class CompactorV2 implements Compactor {
	private static final int NORMAL = 0;
	private static final int STOPPING = 1;
	private static final int STOPPED = 2;
	private static final int COMPACTING = 3;

	ExecutorService exec;

	private SortedLevelFile file;

	private volatile int state = NORMAL;
	private volatile boolean compactLevel0;

	Set<Integer> running = Collections
			.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());

	private volatile boolean started = false;

	public CompactorV2(final SortedLevelFile file, int n) {
		this.file = file;
		exec = Executors.newCachedThreadPool(
		// n,
				new ThreadFactory() {

					@Override
					public Thread newThread(Runnable r) {
						Thread t = Executors.defaultThreadFactory()
								.newThread(r);
						t.setName("CompactorWorker for file " + file);
						return t;
					}
				});
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.bigtextformat.levels.CompactorInterface#waitFinished()
	 */
	@Override
	public synchronized void waitFinished() {
		synchronized (running) {
			while (!running.isEmpty())
				try {
					running.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			exec.shutdown();
		}

		try {
			exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		// changeStateTo(STOPPING);
		// while (!checkState(STOPPED))
		// try {
		// wait();
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// }
	}

	private synchronized void changeStateTo(int newState) {
		state = newState;
		notifyAll();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.bigtextformat.levels.CompactorInterface#start()
	 */
	@Override
	public void start() {
		started = true;
	}

	public void compact(int level) {
		if (!started)
			return;
		synchronized (running) {
			if (running.contains(level))
				return;
			else
				running.add(level);
		}
		exec.execute(new CompactorWorker(this, file, level));

	}

	private synchronized boolean checkState(int s) {
		return state == s;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.bigtextformat.levels.CompactorInterface#setChanged()
	 */
	@Override
	public synchronized void setChanged() {
		if (!checkState(STOPPED) && !checkState(STOPPING))
			changeStateTo(COMPACTING);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.bigtextformat.levels.CompactorInterface#forcecompact()
	 */
	@Override
	public synchronized void forcecompact() {
		waitEmptyRunList();

		compactLevel0 = true;
		int currLevels = file.getMaxLevel();
		for (int i = 0; i <= currLevels; i++) {
			compact(i);
		}
		waitEmptyRunList();

		compactLevel0 = false;
	}

	private void waitEmptyRunList() {
		synchronized (running) {
			while (!running.isEmpty())
				try {
					running.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
		}
	}

	public boolean compactLevel0() {
		return compactLevel0;
	}

	public void removeRunning(int level) {
		synchronized (running) {
			running.remove(level);
			if (running.isEmpty())
				running.notifyAll();
		}
	}
}
