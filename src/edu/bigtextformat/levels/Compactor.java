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
	private boolean finished;

	public Compactor(SortedLevelFile sortedLevelFile) {
		setName("Compactor Thread");
		setDaemon(true);
		this.file = sortedLevelFile;
	}

	@Override
	public void run() {
		while (!file.closed) {
			try {
				check(file.compacting);
				synchronized (this) {
					wait(500);
				}

			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
		setFinished();
	}

	private synchronized void setFinished() {
		finished = true;
		notify();

	}

	public synchronized void waitFinished() {
		notify();
		while (!finished)
			try {
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	}

	public synchronized boolean check(boolean compactLevel0) throws Exception {
		System.out.println("Checking");
		int i = 0;
		boolean found = false;
		LevelFile from = null;
		int level = 0;

		while (i <= file.getMaxLevel() && !found) {
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
						// LevelFile temp = LevelFile.newFile(file.getCwd()
						// .toString(), file.getOpts(), 0, file
						// .getLastLevelIndex(0));
						// LevelFile selected = files.get(0);
						//
						// List<LevelFile> intersect = file.getLevelFile(
						// selected.getMinKey(), selected.getMaxKey(), 0);
						//
						// if (!intersect.contains(selected))
						// intersect.add(selected);
						//
						// List<PairReader> readers = new ArrayList<>();
						// for (LevelFile levelFile : intersect) {
						// readers.add(levelFile.getPairReader());
						// }
						//
						// try {
						//
						// DataBlock db = new DataBlock();
						// LevelFileWriter writer = temp.getWriter();
						//
						// PairReader min = getMin(readers, file.getFormat());
						// while (min != null) {
						// db.add(min.peek().getA(), min.peek().getB());
						// min.next();
						// min = getMin(readers, file.getFormat());
						// }
						//
						// writer.add(db);
						// writer.close();
						// } catch (Exception e) {
						// e.printStackTrace();
						// }
						//
						// temp.commitAndPersist();
						//
						// for (LevelFile levelFile : intersect) {
						// file.delete(levelFile);
						// }
						//
						// file.addLevel(temp);
						//
						// from = temp;

					} else if (i > 0
							&& files.size() > file.getOpts().maxLevelFiles) {
						from = files.get(0);
						level = i;
						found = true;
						// for (int j = 0; j < files.size() && !found; j++) {
						// if (files.get(j).size() > i
						// * file.getOpts().baseSize) {
						// from = files.get(j);
						// level = i;
						// found = true;
						// }
						// }
					}
				}
				i++;
			}
		}
		if (found) {
			merge(from, level);
			return true;
		}
		return false;

	}

	private PairReader getMin(List<PairReader> readers, BlockFormat format) {
		PairReader min = null;
		for (PairReader pairReader : readers) {
			Pair<byte[], byte[]> peek = pairReader.peek();
			if (peek != null) {
				if (min == null
						|| format.compare(min.peek().getA(), peek.getA()) > 0) {
					min = pairReader;
				}
			}
		}
		return min;
	}

	private void merge(LevelFile from, int level) throws Exception {
		// System.out.println(DataTypeUtils.byteArrayToInt(from.getMinKey()));
		// System.out.println(DataTypeUtils.byteArrayToInt(from.getMaxKey()));
		System.out.println("Merging");
		CompactWriter writer = new CompactWriter(file, level + 1);

		List<LevelFile> intersect = file.getLevelFile(from.getMinKey(),
				from.getMaxKey(), level + 1);

		if (intersect.isEmpty()) {
			LevelFileReader r = from.getReader();
			while (r.hasNext()) {
				DataBlock dataBlock = (DataBlock) r.next();
				System.out.println("Esta agregando el bloque entero");
				writer.add(dataBlock);
			}
			writer.persist();
			// file.moveTo(from, level + 1);
			file.delete(from);
			return;
		}

		intersect.remove(from);

		// DataBlock merge = new DataBlock();

		Collections.sort(intersect, new Comparator<LevelFile>() {
			@Override
			public int compare(LevelFile o1, LevelFile o2) {
				return file.getFormat().compare(o1.getMaxKey(), o2.getMaxKey());
			}

		});

		// System.out.println("Merging " + from.print(file.getOpts().format));

		ArrayList<LevelFileReader> readers = new ArrayList<>();
		for (LevelFile levelFile : intersect) {
			// System.out
			// .println("With " + levelFile.print(file.getOpts().format));
			readers.add(levelFile.getReader());
		}

		PairReader fromReader = from.getPairReader();

		Pair<byte[], byte[]> current = fromReader.next();
		BlockFormat format = file.getOpts().format;

		for (LevelFileReader levelFileReader : readers) {
			while (levelFileReader.hasNext()) {
				DataBlock dataBlock = (DataBlock) levelFileReader.next();
				if (current == null
						|| format.compare(dataBlock.lastKey(), current.getA()) < 0) {
					System.out.println("Esta agregando el bloque entero");
					writer.add(dataBlock);
				} else {
					System.out.println("Está comparando par a par.");
					Iterator<Pair<byte[], byte[]>> it = dataBlock.iterator();
					Pair<byte[], byte[]> pair = it.next();
					while (pair != null) {
						if (current == null) {
							writer.add(pair.getA(), pair.getB());
							pair = it.next();
						} else {
							int compare = format.compare(pair.getA(),
									current.getA());
							if (compare < 0) {
								writer.add(pair.getA(), pair.getB());
								pair = it.next();
							} else if (compare == 0) {
								writer.add(current.getA(), current.getB());
								current = fromReader.next();
							} else {
								writer.add(current.getA(), current.getB());
								current = fromReader.next();
							}
						}
					}
				}
			}
		}

		if (current != null || fromReader.hasNext()) {
			if (current != null)
				writer.add(current.getA(), current.getB());
			while (fromReader.hasNext()) {
				current = fromReader.next();
				writer.add(current.getA(), current.getB());
			}
		}

		writer.persist();

		for (LevelFile levelFile : intersect) {
			file.delete(levelFile);
		}
		file.delete(from);
	}
}