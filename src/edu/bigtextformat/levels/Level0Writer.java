package edu.bigtextformat.levels;

final class Level0Writer extends Thread {
	/**
	 * 
	 */
	private final SortedLevelFile sortedFile;

	/**
	 * @param sortedLevelFile
	 */
	Level0Writer(SortedLevelFile sortedLevelFile) {
		setName("Level0 Writer");
		setDaemon(true);
		sortedFile = sortedLevelFile;
	}

	private volatile boolean stop;
	private volatile boolean changed;
	private volatile boolean finished = false;

	@Override
	public synchronized void run() {
		while (!stop) {
			try {
				while (!changed)
					wait();
				changed = false;
				if (stop)
					break;
				sortedFile.writeNextMemtable();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		finished = true;
		notifyAll();
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