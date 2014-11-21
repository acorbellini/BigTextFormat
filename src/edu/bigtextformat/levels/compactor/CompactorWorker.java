package edu.bigtextformat.levels.compactor;

import java.util.HashSet;
import java.util.Iterator;
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
			if ((l.level() == 0 && checkLevel0Conditions(l))
					|| (l.level() > 0 && checkLevelConditions(l))) {
				// System.out.println(file.print());
				// LevelFile from = LevelMerger.shrinkLevel0(l);
				// System.out.println(file.print());
				LevelFile first = l.get(0);
				Set<LevelFile> from = null;
				if (level == 0)
					from = l.intersect(first.getMinKey(), first.getMaxKey());
				else {
					from = new HashSet<>();
					from.add(first);
				}
				// from.add(first);
				// for (int i = 0; from.size() < file.getOpts().minMergeElements
				// && i < l.size(); i++) {
				// from.add(l.get(i));
				// }
				Iterator<LevelFile> it = from.iterator();
				while (it.hasNext()) {
					LevelFile levelFile = (LevelFile) it.next();
					boolean res = levelFile.setMerging(level);
					if (!res) {
						it.remove();
					}
				}

				if (from.isEmpty())
					return false;

				Level next = file.getLevel(level + 1);
				LevelMerger.merge(from, l, next);

				for (LevelFile levelFile : from)
					levelFile.unSetMerging();
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
