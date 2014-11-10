package edu.bigtextformat.levels;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.levels.levelfile.LevelFile;
import edu.bigtextformat.levels.levelfile.LevelFileReader;
import edu.bigtextformat.levels.levelfile.LevelFileWriter;

public class Compactor extends Thread {

	private SortedLevelFile file;
	private volatile boolean finished = false;
	private volatile boolean changed = false;
	private volatile boolean stop = false;

	public Compactor(SortedLevelFile sortedLevelFile) {
		setName("Compactor Thread");
		setDaemon(true);
		this.file = sortedLevelFile;
	}

	@Override
	public synchronized void run() {
		while (!file.closed) {
			try {
				while (!changed)
					wait();
				changed = false;
				if (stop)
					break;
				while (check(false))
					;
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
		finished = true;
		notify();
	}

	public synchronized void setChanged() {
		this.changed = true;
		notify();
	}

	public synchronized void waitFinished() {
		stop = true;
		setChanged();
		notify();
		while (!finished)
			try {
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	}

	int lastLevel = 0;

	public synchronized boolean check(boolean compactLevel0) throws Exception {
		// System.out.println("Checking");
		int cont = lastLevel;
		boolean found = false;
		LevelFile from = null;
		int level = 0;
		// for (i = lastLevel; i != ((lastLevel % (maxLevel + 1))) + 1 &&
		// !found; i = (i % (maxLevel + 1)) + 1) {
		int maxLevel = file.getMaxLevel();
		while (cont <= lastLevel + maxLevel && !found) {
			int i = cont % (maxLevel + 1);
			List<LevelFile> files = file.getLevel(i);
			if (files != null) {
				synchronized (files) {
					if (i == 0
							&& files.size() > 0
							&& (compactLevel0 || files.size() >= file.getOpts().compactLevel0Threshold)) {
						from = files.get(0);
						found = true;
						level = i;

						// System.out.println("Creating temp file");
						LevelFile temp = LevelFile.newFile(file.getCwd()
								.toString(), file.getOpts(), 0, file
								.getLastLevelIndex(0));
						LevelFile selected = files.get(0);

						List<LevelFile> intersect = file.getLevelFile(
								selected.getMinKey(), selected.getMaxKey(), 0);

						if (!intersect.contains(selected))
							intersect.add(selected);

						List<PairReader> readers = new ArrayList<>();
						for (LevelFile levelFile : intersect) {
							PairReader pairReader = levelFile.getPairReader();
							readers.add(pairReader);
							pairReader.advance();
						}

						try {
							DataBlockWriter db = new DataBlockWriter();
							LevelFileWriter writer = temp.getWriter();
							PairReader min = getMin(readers, file.getFormat());
							while (min != null && min.getKey() != null) {
								db.add(min.getKey(), min.getValue());
								min.advance();
								min = getMin(readers, file.getFormat());
							}
							DataBlockIterator it = db.getDB().iterator();
							while (it.hasNext()) {
								it.advance();
								writer.add(it.getKey(), it.getVal());
							}
							writer.close();
						} catch (Exception e) {
							e.printStackTrace();
						}

						temp.commitAndPersist();

						for (LevelFile levelFile : intersect) {
							file.delete(levelFile);
						}

						file.addLevel(temp);

						from = temp;

					} else if (i > 0
							&& files.size() > file.getOpts().maxLevelFiles) {
						from = files.get(0);
						level = i;
						found = true;
					}
				}
				cont++;
			}
		}

		lastLevel = cont % (maxLevel + 1);

		if (found) {
			merge(from, level);
			return true;
		}
		return false;

	}

	private PairReader getMin(List<PairReader> readers, BlockFormat format) {
		PairReader min = null;
		for (PairReader pairReader : readers) {
			if (pairReader.getKey() != null) {
				if (min == null
						|| format.compare(min.getKey(), pairReader.getKey()) > 0) {
					min = pairReader;
				}
			}
		}
		return min;
	}

	private void merge(LevelFile from, int level) throws Exception {
		// System.out.println("Merging");
		CompactWriter writer = new CompactWriter(file, level + 1);

		List<LevelFile> intersect = file.getLevelFile(from.getMinKey(),
				from.getMaxKey(), level + 1);

		if (intersect.isEmpty()) {
			// LevelFileReader r = from.getReader();
			// while (r.hasNext()) {
			// DataBlock dataBlock = (DataBlock) r.next();
			// // System.out.println("Esta agregando el bloque entero");
			// writer.add(dataBlock);
			// }
			// writer.persist();
			// file.delete(from);
			file.moveTo(from, level + 1);
			return;
		}

		intersect.remove(from);

		Collections.sort(intersect, new Comparator<LevelFile>() {
			@Override
			public int compare(LevelFile o1, LevelFile o2) {
				return file.getFormat().compare(o1.getMaxKey(), o2.getMaxKey());
			}

		});

		ArrayList<LevelFileReader> readers = new ArrayList<>();
		for (LevelFile levelFile : intersect) {
			readers.add(levelFile.getReader());
		}

		PairReader fromReader = from.getPairReader();

		BlockFormat format = file.getOpts().format;

		while (fromReader.hasNext()) {
			fromReader.advance();
			for (LevelFileReader levelFileReader : readers) {
				while (levelFileReader.hasNext()) {
					DataBlock dataBlock = (DataBlock) levelFileReader.next();
					if (fromReader.getKey() == null
							|| format.compare(dataBlock.lastKey(),
									fromReader.getKey()) < 0) {
						writer.add(dataBlock);
					} else {
						DataBlockIterator it = dataBlock.iterator();
						it.advance();
						while (it.getKey() != null) {
							if (fromReader.getKey() == null) {
								writer.add(it.getKey(), it.getVal());
								it.advance();
							} else {
								int compare = format.compare(it.getKey(),
										fromReader.getKey());
								if (compare < 0) {
									writer.add(it.getKey(), it.getVal());
									it.advance();
								} else {
									writer.add(fromReader.getKey(),
											fromReader.getValue());
									fromReader.advance();

								}
							}
						}
					}
				}
			}
			if (fromReader.getKey() != null)
				writer.add(fromReader.getKey(), fromReader.getValue());
		}

		writer.persist();

		for (LevelFile levelFile : intersect) {
			file.delete(levelFile);
		}
		file.delete(from);
	}
}