package edu.bigtextformat.levels;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

final class Level0Writer {
	/**
	 * 
	 */
	private final SortedLevelFile sortedFile;
	private volatile boolean stop;
	private volatile boolean changed;
	private volatile boolean finished = false;
	private ExecutorService exec;
	private Semaphore sem;

	/**
	 * @param sortedLevelFile
	 */
	Level0Writer(SortedLevelFile sortedLevelFile) {
		sortedFile = sortedLevelFile;
		exec = Executors
				.newFixedThreadPool(sortedFile.getOpts().maxWriterThreads);
		sem = new Semaphore(sortedFile.getOpts().maxWriterThreads);
	}

	public void start() {
		Thread t = new Thread("Level0 Writer") {
			public void run() {
				exec();
			};
		};
		t.setDaemon(true);
		t.start();
	}

	public void exec() {
		while (!stop) {
			try {
				synchronized (this) {
					while (!changed)
						wait();
					changed = false;
				}
				if (stop)
					break;
				sem.acquire();
				exec.execute(new Runnable() {
					@Override
					public void run() {
						try {
							sortedFile.writeNextMemtable();
						} catch (Exception e) {
							e.printStackTrace();
						} finally {
							sem.release();
						}
					}
				});

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		exec.shutdown();
		try {
			exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		synchronized (this) {
			finished = true;
			notifyAll();
		}

	}

	public synchronized void setChanged() {
		this.changed = true;
		notify();
	}

	public synchronized void waitFinished() {
		stop = true;
		setChanged();
		while (!finished)
			try {
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

	}
}