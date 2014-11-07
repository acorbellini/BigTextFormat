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
			DataBlock table = sortedFile.getNextMemtable();
			if (table != null) {
				try {
					sortedFile.writeLevel0(table, false);
				} catch (Exception e) {
					e.printStackTrace();
				}
				sortedFile.removeMemtable(table);
			}
		}
	}
}