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

	@Override
	public void run() {
		while (!sortedFile.closed) {
			try {
				sortedFile.writeNextMemtable();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}