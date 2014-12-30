package edu.bigtextformat.levels.memtable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

public class MemtableSegment {
	private volatile Memtable current;
	private List<Future<Void>> fut = new ArrayList<Future<Void>>();
	private ExecutorService exec;
	private volatile Memtable old;

	public MemtableSegment(int maxwriter) {
		exec = Executors.newFixedThreadPool(maxwriter, new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				ThreadFactory tf = Executors.defaultThreadFactory();
				Thread t = tf.newThread(r);
				t.setName("Segment Writer");
				t.setDaemon(true);
				return t;
			}
		});
	}

	public Memtable getCurrent() {
		old = current;
		return current;
	}

	public ExecutorService getExec() {
		return exec;
	}

	public List<Future<Void>> getFut() {
		return fut;
	}

	public void setCurrent(Memtable current) {
		this.current = current;
	}

	public Memtable getOld() {
		return old;
	}
}