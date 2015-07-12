package edu.bigtextformat.levels;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import org.apache.log4j.Logger;

import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.levels.compactor.LevelMerger;
import edu.bigtextformat.levels.levelfile.LevelFile;

public class LevelRepairer {
	Logger log = Logger.getLogger(LevelRepairer.class);

	private SortedLevelFile file;

	public LevelRepairer(SortedLevelFile file) {
		this.file = file;
	}

	public void repair(Levels levels) throws Exception {
		ExecutorService execRec = Executors.newFixedThreadPool(
				file.getOpts().recoveryThreads, new ThreadFactory() {

					@Override
					public Thread newThread(Runnable r) {
						ThreadFactory tf = Executors.defaultThreadFactory();
						Thread t = tf.newThread(r);
						t.setName("Recovery Thread for " + file);
						t.setDaemon(true);
						return t;
					}
				});

		final ExecutorService execCR = Executors.newFixedThreadPool(
				file.getOpts().recoveryWriters, new ThreadFactory() {

					@Override
					public Thread newThread(Runnable r) {
						ThreadFactory tf = Executors.defaultThreadFactory();
						Thread t = tf.newThread(r);
						t.setName("Recovery Writer for " + file);
						t.setDaemon(true);
						return t;
					}
				});

		List<Future<Void>> futures = new ArrayList<Future<Void>>();

		List<LevelFile> current = new ArrayList<LevelFile>();
		boolean clean = false;
		while (!clean) {
			clean = true;
			futures.clear();
			current.clear();
			for (final Level to : levels) {
				if (to.level() > 0) {
					List<LevelFile> files = to.files();

					for (int i = 0; i < files.size(); i++) {
						LevelFile levelFile = files.get(i);

						if (current.contains(levelFile))
							continue;
						final Set<LevelFile> intersection = getConsecutiveIntersection(
								files, i, file.getOpts().recoveryMaxIntersect,
								file.getOpts().format);

						if (intersection.size() > 1) {

							clean = false;

							current.addAll(intersection);

							futures.add(execRec.submit(new Callable<Void>() {

								@Override
								public Void call() throws Exception {
									log.info("Recovering failed merge among "
											+ intersection.size()
											+ " files on level " + to.level()
											+ " of db " + file);
									try {
										LevelMerger.shrink(to, intersection,
												execCR);
									} catch (Exception e) {
										e.printStackTrace();
									}
									return null;
								}
							}));
						}
					}
				}
			}
			for (Future<Void> future : futures) {
				future.get();
			}
		}
		execCR.shutdown();
		execRec.shutdown();
	}

	public static Set<LevelFile> getConsecutiveIntersection(List<LevelFile> l,
			int init, int max, BlockFormat format) throws Exception {
		LevelFile first = l.get(init);
		final Set<LevelFile> intersection = new HashSet<LevelFile>();
		intersection.add(first);
		int j = init;
		boolean done = false;
		byte[] maxKey = first.getMaxKey();
		while (intersection.size() < max && j < l.size() - 1 && !done) {
			LevelFile other = l.get(j + 1);
			if (other.intersectsWith(first.getMinKey(), maxKey)) {
				intersection.add(other);
				j++;
				if (format.compare(other.getMaxKey(), maxKey) > 0)
					maxKey = other.getMaxKey();
			} else
				done = true;
		}
		return intersection;
	}
}
