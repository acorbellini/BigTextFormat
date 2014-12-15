package edu.bigtextformat.levels.compactor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.levels.DataBlock;
import edu.bigtextformat.levels.DataBlockIterator;
import edu.bigtextformat.levels.DataBlockWriter;
import edu.bigtextformat.levels.Level;
import edu.bigtextformat.levels.PairReader;
import edu.bigtextformat.levels.SingleFileWriter;
import edu.bigtextformat.levels.levelfile.LevelFile;
import edu.bigtextformat.levels.levelfile.LevelFileReader;
import edu.bigtextformat.levels.levelfile.LevelFileWriter;

public class LevelMerger {

	private static final int RATE = (int) ((512 * 1024) / 1000); // 1MB
																	// per
																	// sec

	public static void shrink(Level level, Set<LevelFile> level0Merge,
			ExecutorService execCR) throws Exception {
		List<PairReader> readers = new ArrayList<>();
		for (LevelFile levelFile : level0Merge) {
			try {
				PairReader pairReader;
				pairReader = levelFile.getPairReader();
				readers.add(pairReader);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		try {
			DataBlockWriter db = new DataBlockWriter();
			// LevelFileWriter writer = temp.getWriter();
			CompactWriterV3 writer = new CompactWriterV3(level, execCR);
			// writer.setTrottle(RATE);
			PairReader min = getNext(readers, level.getOpts().format, null);
			while (min != null && min.getKey() != null) {
				byte[] key = min.getKey();
				db.add(key, min.getValue());
				min.advance();
				min = getNext(readers, level.getOpts().format, key);
			}
			DataBlockIterator it = db.getDB().iterator();
			while (it.hasNext()) {
				writer.add(it.getKey(), it.getVal());
				it.advance();
			}
			writer.persist();
		} catch (Exception e) {
			e.printStackTrace();
		}

		// temp.commitAndPersist();
		// level.add(temp);

		for (LevelFile levelFile : level0Merge) {
			level.delete(levelFile);
		}

		// return temp;
	}

	public static void merge(Set<LevelFile> list, final Level current,
			Level to, boolean trottle, ExecutorService exec) throws Exception {
		// long init = System.currentTimeMillis();
		BlockFormat format = current.getOpts().format;

		// for (LevelFile levelFile : list) {
		// System.out.println("From " + levelFile + " min key: "
		// + format.print(levelFile.getMinKey()) + " max key: "
		// + format.print(levelFile.getMaxKey()));
		// }

		Set<LevelFile> intersect = new HashSet<>();
		for (LevelFile from : list) {
			intersect.addAll(to.intersect(from.getMinKey(), from.getMaxKey()));
		}

		// if (intersect.size() > 12)

		// int levelFileToMerge = 0;
		// while (intersect.size() < Math.max(to.getOpts().minMergeElements,
		// to.size() - to.getOpts().maxLevelFiles)
		// && levelFileToMerge < to.size()) {
		// intersect.add(to.get(levelFileToMerge++));
		// }

		// intersect.remove(from);
		Iterator<LevelFile> itIntersection = intersect.iterator();
		while (itIntersection.hasNext()) {
			LevelFile levelFile = (LevelFile) itIntersection.next();
			boolean res = levelFile.setMerging(to.level());
			if (!res)
				itIntersection.remove();
		}
		if (intersect.isEmpty() && list.size() == 1) {
			LevelFile next = list.iterator().next();
			current.moveTo(next, to);
			next.unSetMerging();

			// System.out.println("Moved " + next + " min key: "
			// + format.print(next.getMinKey()) + " max key: "
			// + format.print(next.getMaxKey()));

			return;
		}

		// System.out.println("Merging INTERSECTION: " + intersect.size()
		// + " LIST ELEMENTS: " + list.size() + " elements.");

		Writer writer = null;
		if (current.getOpts().splitMergedFiles) {
//			writer = new CompactWriterV2(to);
			writer = new CompactWriterV3(to, exec);
			if (trottle)
				((CompactWriterV3) writer).setTrottle(RATE);
		} else {
			writer = new SingleFileWriter(to);
		}

		ArrayList<LevelFile> intersectSorted = new ArrayList<>(intersect);

		Collections.sort(intersectSorted, new Comparator<LevelFile>() {
			@Override
			public int compare(LevelFile o1, LevelFile o2) {
				try {
					return current.getOpts().format.compare(o1.getMaxKey(),
							o2.getMaxKey());
				} catch (Exception e) {
					e.printStackTrace();
				}
				return 0;
			}

		});

		// for (LevelFile levelFile : intersectSorted) {
		// System.out.println("File " + levelFile + " min key: "
		// + format.print(levelFile.getMinKey()) + " max key: "
		// + format.print(levelFile.getMaxKey()));
		// }

		ArrayList<LevelFileReader> readers = new ArrayList<>();
		for (LevelFile levelFile : intersectSorted) {
			try {
				readers.add(levelFile.getReader());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		List<PairReader> fromReaders = new ArrayList<PairReader>();
		for (LevelFile from : list) {
			fromReaders.add(from.getPairReader());
		}

		PairReader fromReader = getNext(fromReaders, format, null);
		// System.out.println("First FROM key: "
		// + format.print(fromReader.getKey()));

		for (LevelFileReader levelFileReader : readers) {
			// System.out.println("Current file " + levelFileReader.getFile());
			while (levelFileReader.hasNext()) {
				DataBlock dataBlock = levelFileReader.next();
				// System.out.println("Current block: " +
				// dataBlock.print(format));
				if (fromReader == null
						|| format.compare(dataBlock.lastKey(),
								fromReader.getKey()) < 0) {
					// System.out.println("Writing block: "
					// + dataBlock.print(format));
					writer.addDataBlock(dataBlock);
				} else {
					try {
						DataBlockIterator it = dataBlock.iterator();
						while (it.hasNext()) {
							if (fromReader == null) {
								writer.add(it.getKey(), it.getVal());
								it.advance();
							} else {
								int compare = format.compare(it.getKey(),
										fromReader.getKey());
								if (compare < 0) {
									// System.out.println("Block key "
									// + format.print(it.getKey())
									// + " is less than "
									// + format.print(fromReader.getKey()));
									writer.add(it.getKey(), it.getVal());
									it.advance();
								} else {
									// System.out.println("Block key "
									// + format.print(it.getKey())
									// + " is greater or equal than "
									// + format.print(fromReader.getKey()));
									if (compare == 0) {
										// System.out
										// .println("It was equal, ignoring block key.");
										it.advance();
									}
									byte[] key = fromReader.getKey();
									writer.add(key, fromReader.getValue());
									fromReader.advance();
									fromReader = getNext(fromReaders, format,
											key);

								}
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}

		while (fromReader != null) {
			byte[] key = fromReader.getKey();
			writer.add(key, fromReader.getValue());
			fromReader.advance();
			fromReader = getNext(fromReaders, format, key);
		}

		writer.persist();

		// System.out.println("Generated:");
		// for (LevelFile levelFile : writer.getFiles()) {
		// System.out.println("From " + levelFile + " min key: "
		// + format.print(levelFile.getMinKey()) + " max key: "
		// + format.print(levelFile.getMaxKey()));
		// }

		for (LevelFile levelFile : intersectSorted) {
			to.delete(levelFile);
		}
		for (LevelFile from : list) {
			current.delete(from);
		}
		// System.out.println("Merge from Level " + current.level() + " to "
		// + to.level() + " time: " + (System.currentTimeMillis() - init));
	}

	public static LevelFile shrink(final Level files) throws Exception {
		Set<LevelFile> intersect = new HashSet<LevelFile>();
		// if (files.size() >= file.getOpts().minMergeElements)
		// {
		int levelFileToMerge = 0;
		while (intersect.size() < Math.max(files.getOpts().minMergeElements,
				files.size() - files.getOpts().maxLevelFiles)
				&& levelFileToMerge < files.size()) {
			intersect.add(files.get(levelFileToMerge++));
		}

		if (intersect.size() == 1)
			return intersect.iterator().next();

		LevelFile temp = LevelFile.newFile(files.getCwd().toString(),
				files.getOpts(), files.level(), files.getLastLevelIndex());

		LevelFileWriter writer = temp.getWriter();

		ArrayList<LevelFile> intersectSorted = new ArrayList<>(intersect);
		sortByKey(intersectSorted, files.getOpts().format);
		ArrayList<LevelFileReader> readers = new ArrayList<>();
		for (LevelFile levelFile : intersectSorted) {
			readers.add(levelFile.getReader());
		}
		for (LevelFileReader levelFileReader : readers) {
			while (levelFileReader.hasNext()) {
				DataBlock dataBlock = levelFileReader.next();
				writer.addDatablock(dataBlock);
			}
		}
		writer.close();

		temp.commitAndPersist();
		files.add(temp);
		for (LevelFile levelFile : intersectSorted) {
			files.delete(levelFile);
		}
		return temp;
	}

	public static void sortByKey(List<LevelFile> intersectSorted,
			final BlockFormat format) {
		Collections.sort(intersectSorted, new Comparator<LevelFile>() {
			@Override
			public int compare(LevelFile o1, LevelFile o2) {
				try {
					return format.compare(o1.getMaxKey(), o2.getMaxKey());
				} catch (Exception e) {
					e.printStackTrace();
				}
				return 0;
			}

		});
	}

	public static PairReader getNext(List<PairReader> readers,
			BlockFormat format, byte[] before) {
		PairReader min = null;
		Iterator<PairReader> it = readers.iterator();
		while (it.hasNext()) {
			PairReader pairReader = (PairReader) it.next();
			while (pairReader.hasNext()
					&& (before != null && format.compare(before,
							pairReader.getKey()) >= 0))
				pairReader.advance();
			if (pairReader.hasNext()) {
				if (min == null)
					min = pairReader;
				else {
					int compare = format.compare(min.getKey(),
							pairReader.getKey());
					if (compare == 0
							&& min.getReader().getFile().getCont() < pairReader
									.getReader().getFile().getCont())
						min = pairReader;
					else if (compare > 0)
						min = pairReader;
				}
			} else
				it.remove();
		}

		return min;
	}
}
