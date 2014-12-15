package edu.bigtextformat.levels.compactor;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import edu.bigtextformat.levels.SortedLevelFile;

public class Compactor {
	private static final int NORMAL = 0;
	private static final int STOPPING = 1;
	private static final int STOPPED = 2;
	private static final int COMPACTING = 3;

	ExecutorService exec;

	private SortedLevelFile file;

	private volatile int state = NORMAL;
	private volatile boolean forceCompact;

	Set<Integer> running = Collections
			.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());

	private volatile boolean started = false;
	private ExecutorService execWriter;

	public Compactor(final SortedLevelFile file) {
		this.file = file;
		exec = Executors.newFixedThreadPool(file.getOpts().maxCompactorThreads, new ThreadFactory() {

			@Override
			public Thread newThread(Runnable r) {
				Thread t = Executors.defaultThreadFactory().newThread(r);
				t.setName("CompactorWorker for file " + file);
				return t;
			}
		});

		execWriter = Executors.newFixedThreadPool(
				file.getOpts().maxCompactionWriters, new ThreadFactory() {
					@Override
					public Thread newThread(Runnable r) {
						ThreadFactory tf = Executors.defaultThreadFactory();
						Thread t = tf.newThread(r);
						t.setName("Compact Writer for " + file);
						t.setDaemon(true);
						return t;
					}
				});
		;
	}

	private synchronized void changeStateTo(int newState) {
		state = newState;
		notifyAll();
	}

	private synchronized boolean checkState(int s) {
		return state == s;
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
		exec.execute(new CompactorWorker(this, file, level, execWriter));

	}

	public synchronized void forcecompact() {
		forceCompact = true;
		waitEmptyRunList();
		int currLevels = file.getMaxLevel();
		for (int i = 0; i <= currLevels; i++) {
			compact(i);
		}
		waitEmptyRunList();

		forceCompact = false;
	}

	public boolean isForceCompact() {
		return forceCompact;
	}

	public void removeRunning(int level) {
		synchronized (running) {
			running.remove(level);
			if (running.isEmpty())
				running.notifyAll();
		}
	}

	public synchronized void setChanged() {
		if (!checkState(STOPPED) && !checkState(STOPPING))
			changeStateTo(COMPACTING);
	}

	public void start() {
		started = true;
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
		execWriter.shutdown();
		try {
			execWriter.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
