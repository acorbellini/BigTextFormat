package edu.bigtextformat.levels.compactor;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import edu.bigtextformat.levels.Level;
import edu.bigtextformat.levels.SortedLevelFile;
import edu.bigtextformat.levels.levelfile.LevelFile;

public class CompactorWorker implements Runnable {

	private SortedLevelFile file;
	private int level;
	private Compactor compactor;
	ExecutorService exec;

	Set<LevelFile> from = new HashSet<>();

	public CompactorWorker(Compactor compactor, SortedLevelFile file,
			int level, ExecutorService exec) {
		this.file = file;
		this.level = level;
		this.compactor = compactor;
		this.exec = exec;
	}

	@Override
	public void run() {
		try {
			while (exec())
				;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			compactor.removeRunning(level);
		}
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
				LevelFile first = l.getRandom();

				if (level == 0) {
					// if (compactor.isForceCompact())
					// from = l.intersect(first.getMinKey(), first.getMaxKey());
					// else
					from.addAll(l.intersect(first.getMinKey(),
							first.getMaxKey(), file.getOpts().maxMergeElements));

				} else {
					from.add(first);
				}
				// from.add(first);
				// for (int i = 0; from.size() < file.getOpts().minMergeElements
				// && i < l.size(); i++) {
				// from.add(l.get(i));
				// }

				boolean removed = false;
				Iterator<LevelFile> it = from.iterator();
				while (it.hasNext()) {
					LevelFile levelFile = (LevelFile) it.next();
					boolean res = levelFile.setMerging(level);
					if (!res) {
						it.remove();
						removed = true;
					}
				}

				if (from.isEmpty())
					if (removed)
						return true;
					else
						return false;

				Level next = file.getLevel(level + 1);
				LevelMerger.merge(from, l, next, !compactor.isForceCompact(),
						exec);

				for (LevelFile levelFile : from)
					levelFile.unSetMerging();
				return true;
			}
			return false;
		} catch (Exception e) {
			throw e;
		} finally {
			from.clear();
		}
	}

	private boolean checkLevel0Conditions(Level files) {
		return (files.size() > 0 && (compactor.isForceCompact() || files.size() >= file
				.getOpts().compactLevel0Threshold));
	}

	private boolean checkLevelConditions(Level files) {
		return files.size() > Math.pow(file.getOpts().maxLevelFiles, level);
	}

}
