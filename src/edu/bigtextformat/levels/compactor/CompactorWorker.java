package edu.bigtextformat.levels.compactor;

import java.util.HashSet;
import java.util.Set;

import edu.bigtextformat.levels.Level;
import edu.bigtextformat.levels.SortedLevelFile;
import edu.bigtextformat.levels.levelfile.LevelFile;

public class CompactorWorker implements Runnable {

	private SortedLevelFile file;
	private int level;
	private CompactorV2 compactor;

	public CompactorWorker(CompactorV2 compactor, SortedLevelFile file,
			int level) {
		this.file = file;
		this.level = level;
		this.compactor = compactor;
	}

	@Override
	public void run() {
		try {
			while (exec())
				;
		} catch (Exception e) {
			e.printStackTrace();
		}
		compactor.removeRunning(level);
	}

	private Boolean exec() throws Exception {
		try {
			Level l = file.getLevel(level);
			if (l == null)
				return false;
			if (l.level() == 0 && checkLevel0Conditions(l)) {
				// System.out.println(file.print());
				// LevelFile from = LevelMerger.shrinkLevel0(l);
				// System.out.println(file.print());
				LevelFile first = l.get(0);
				Set<LevelFile> from = l.intersect(first.getMinKey(),
						first.getMaxKey());

				Level next = file.getLevel(1);
				synchronized (next) {
					LevelMerger.merge(from, l, next);
				}
				// System.out.println(file.print());
				return true;
			} else if (l.level() > 0 && checkLevelConditions(l)) {
				Level next = file.getLevel(level + 1);
				synchronized (l) {
					LevelFile from = LevelMerger.shrink(l);
					// LevelFile from = l.get(0);
					HashSet<LevelFile> fromSet = new HashSet<>();
					fromSet.add(from);

					synchronized (next) {
						LevelMerger.merge(fromSet, l, next);
					}
				}
				return true;
			}
			return false;
		} catch (Exception e) {
			throw e;
		}
	}

	private boolean checkLevel0Conditions(Level files) {
		return (files.size() > 0 && (compactor.compactLevel0() || files.size() >= file
				.getOpts().compactLevel0Threshold));
	}

	private boolean checkLevelConditions(Level files) {
		return files.size() > file.getOpts().maxLevelFiles;
	}

}
