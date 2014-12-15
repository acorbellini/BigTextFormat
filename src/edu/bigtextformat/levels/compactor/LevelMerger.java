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
import edu.bigtextformat.levels.Level;
import edu.bigtextformat.levels.PairReader;
import edu.bigtextformat.levels.datablock.DataBlock;
import edu.bigtextformat.levels.datablock.DataBlockIterator;
import edu.bigtextformat.levels.datablock.DataBlockWriter;
import edu.bigtextformat.levels.levelfile.LevelFile;
import edu.bigtextformat.levels.levelfile.LevelFileReader;

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
			CompactWriter writer = new CompactWriter(level, execCR);
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

		for (LevelFile levelFile : level0Merge) {
			level.delete(levelFile);
		}
	}

	public static void merge(Set<LevelFile> list, final Level current,
			Level to, boolean trottle, ExecutorService exec) throws Exception {
		BlockFormat format = current.getOpts().format;
		Set<LevelFile> intersect = new HashSet<>();
		for (LevelFile from : list) {
			intersect.addAll(to.intersect(from.getMinKey(), from.getMaxKey()));
		}
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
			return;
		}
		CompactWriter writer = new CompactWriter(to, exec);
		if (trottle)
			((CompactWriter) writer).setTrottle(RATE);

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

		ArrayList<LevelFileReader> intersectionReaders = new ArrayList<>();
		for (LevelFile levelFile : intersectSorted) {
			try {
				intersectionReaders.add(levelFile.getReader());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		List<PairReader> fromReaders = new ArrayList<PairReader>();
		for (LevelFile from : list) {
			fromReaders.add(from.getPairReader());
		}

		PairReader fromReader = getNext(fromReaders, format, null);
		for (LevelFileReader levelFileReader : intersectionReaders) {
			while (levelFileReader.hasNext()) {
				DataBlock dataBlock = levelFileReader.next();
				if (fromReader == null
						|| format.compare(dataBlock.lastKey(),
								fromReader.getKey()) < 0) {
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
									writer.add(it.getKey(), it.getVal());
									it.advance();
								} else {
									if (compare == 0) {
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

		for (LevelFile levelFile : intersectSorted) {
			to.delete(levelFile);
		}
		for (LevelFile from : list) {
			current.delete(from);
		}
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
