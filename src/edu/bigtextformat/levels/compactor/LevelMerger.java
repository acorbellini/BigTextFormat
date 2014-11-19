package edu.bigtextformat.levels.compactor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.levels.DataBlock;
import edu.bigtextformat.levels.DataBlockIterator;
import edu.bigtextformat.levels.DataBlockWriter;
import edu.bigtextformat.levels.Level;
import edu.bigtextformat.levels.PairReader;
import edu.bigtextformat.levels.levelfile.LevelFile;
import edu.bigtextformat.levels.levelfile.LevelFileReader;
import edu.bigtextformat.levels.levelfile.LevelFileWriter;

public class LevelMerger {

	public static LevelFile shrinkLevel0(Level level) throws Exception {

		LevelFile selected = level.get(0);

		Set<LevelFile> level0Merge = level.intersect(selected.getMinKey(),
				selected.getMaxKey());

		if (!level0Merge.contains(selected))
			level0Merge.add(selected);
		// if (files.size() >= file.getOpts().minMergeElements)
		// {
		// int levelFileToMerge = 0;
		// int levelSize = level.size();
		// while (level0Merge.size() <
		// Math.max(level.getOpts().minMergeElements,
		// levelSize - level.getOpts().maxLevelFiles)
		// && levelFileToMerge < levelSize) {
		// level0Merge.add(level.get(levelFileToMerge++));
		// }

		if (level0Merge.size() == 1)
			return selected;

		LevelFile temp = LevelFile.newFile(level.getCwd().toString(),
				level.getOpts(), 0, level.getLastLevelIndex());

		List<PairReader> readers = new ArrayList<>();
		for (LevelFile levelFile : level0Merge) {
			PairReader pairReader = levelFile.getPairReader();
			readers.add(pairReader);
			pairReader.advance();
		}

		try {
			DataBlockWriter db = new DataBlockWriter();
			LevelFileWriter writer = temp.getWriter();
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
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		temp.commitAndPersist();
		level.add(temp);

		for (LevelFile levelFile : level0Merge) {
			level.delete(levelFile);
		}

		return temp;
	}

	public static void merge(Set<LevelFile> list, final Level current, Level to)
			throws Exception {
		Set<LevelFile> intersect = new HashSet<>();
		for (LevelFile from : list) {
			intersect.addAll(to.intersect(from.getMinKey(), from.getMaxKey()));
		}

		// int levelFileToMerge = 0;
		// while (intersect.size() < Math.max(to.getOpts().minMergeElements,
		// to.size() - to.getOpts().maxLevelFiles)
		// && levelFileToMerge < to.size()) {
		// intersect.add(to.get(levelFileToMerge++));
		// }

		// intersect.remove(from);

		if (intersect.isEmpty() && list.size() == 1) {
			current.moveTo(list.iterator().next(), to);
			return;
		}
		CompactWriterV2 writer = new CompactWriterV2(to);

		ArrayList<LevelFile> intersectSorted = new ArrayList<>(intersect);

		Collections.sort(intersectSorted, new Comparator<LevelFile>() {
			@Override
			public int compare(LevelFile o1, LevelFile o2) {
				try {
					return current.getOpts().format.compare(o1.getMaxKey(),
							o2.getMaxKey());
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return 0;
			}

		});

		ArrayList<LevelFileReader> readers = new ArrayList<>();
		for (LevelFile levelFile : intersectSorted) {
			readers.add(levelFile.getReader());
		}

		BlockFormat format = current.getOpts().format;

		List<PairReader> fromReaders = new ArrayList<PairReader>();
		for (LevelFile from : list) {
			fromReaders.add(from.getPairReader());
		}

		PairReader fromReader = getNext(fromReaders, format, null);
		// fromReader.advance();
		for (LevelFileReader levelFileReader : readers) {
			while (levelFileReader.hasNext()) {
				DataBlock dataBlock = levelFileReader.next();
				// if(dataBlock.indexSize()==1)
				// System.out.println("Bloque de 1");
				if (fromReader == null
						|| format.compare(dataBlock.lastKey(),
								fromReader.getKey()) < 0) {
					writer.add(dataBlock);
				} else {
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
								if (compare == 0)
									it.advance();
								byte[] key = fromReader.getKey();
								writer.add(key, fromReader.getValue());
								fromReader.advance();
								fromReader = getNext(fromReaders, format, key);

							}
						}
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
		Collections.sort(intersectSorted, new Comparator<LevelFile>() {
			@Override
			public int compare(LevelFile o1, LevelFile o2) {
				try {
					return files.getOpts().format.compare(o1.getMaxKey(),
							o2.getMaxKey());
				} catch (Exception e) {
					e.printStackTrace();
				}
				return 0;
			}

		});
		ArrayList<LevelFileReader> readers = new ArrayList<>();
		for (LevelFile levelFile : intersectSorted) {
			readers.add(levelFile.getReader());
		}
		for (LevelFileReader levelFileReader : readers) {
			while (levelFileReader.hasNext()) {
				DataBlock dataBlock = levelFileReader.next();
				writer.add(dataBlock);
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

	public static PairReader getNext(List<PairReader> readers,
			BlockFormat format, byte[] before) {
		PairReader min = null;
		for (PairReader pairReader : readers) {
			while (pairReader.hasNext()
					&& (before != null && format.compare(before,
							pairReader.getKey()) >= 0))
				pairReader.advance();
			if (pairReader.hasNext()) {
				if (min == null
						|| format.compare(min.getKey(), pairReader.getKey()) > 0) {
					min = pairReader;
				}
			}
		}

		return min;
	}
}
