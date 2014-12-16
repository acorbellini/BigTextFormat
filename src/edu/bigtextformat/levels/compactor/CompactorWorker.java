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

	private boolean checkLevel0Conditions(Level files) {
		return (files.size() > 0 && (compactor.isForceCompact() || files.size() >= file
				.getOpts().compactLevel0Threshold));
	}

	private boolean checkLevelConditions(Level files) {
		return files.size() > Math.pow(file.getOpts().maxLevelFiles, level);
	}

	private Boolean exec() throws Exception {
		try {
			Level l = file.getLevel(level);
			if (l == null)
				return false;
			if ((l.level() == 0 && checkLevel0Conditions(l))
					|| (l.level() > 0 && checkLevelConditions(l))) {
				if (level == 0) {
					LevelFile curr = l.get(0);
					from.add(curr);
					int j = 0;
					boolean done = false;
					while (!done
							&& from.size() <= file.getOpts().maxMergeElements) {
						LevelFile other = l.get(j + 1);
						if (other != null && curr.intersectsWith(other)) {
							from.add(other);
							curr = other;
							j++;
						} else
							done = true;
					}
				} else
					from.add(l.getRandom());

				Level next = file.getLevel(level + 1);
				LevelMerger.merge(from, l, next, !compactor.isForceCompact(),
						exec);
				return true;
			}
			return false;
		} catch (Exception e) {
			throw e;
		} finally {
			from.clear();
		}
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

}
