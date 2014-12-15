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

import edu.bigtextformat.levels.compactor.LevelMerger;
import edu.bigtextformat.levels.levelfile.LevelFile;

public class LevelRepairer {

	private String path;

	public LevelRepairer(String path) {
		this.path = path;
	}

	public void repair(Levels levels) throws Exception {
		ExecutorService execRec = Executors.newFixedThreadPool(4,
				new ThreadFactory() {

					@Override
					public Thread newThread(Runnable r) {
						ThreadFactory tf = Executors.defaultThreadFactory();
						Thread t = tf.newThread(r);
						t.setName("Recovery Thread for " + path);
						t.setDaemon(true);
						return t;
					}
				});

		final ExecutorService execCR = Executors.newFixedThreadPool(4,
				new ThreadFactory() {

					@Override
					public Thread newThread(Runnable r) {
						ThreadFactory tf = Executors.defaultThreadFactory();
						Thread t = tf.newThread(r);
						t.setName("Recovery Writer for " + path);
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

						final Set<LevelFile> intersection = new HashSet<LevelFile>();
						intersection.add(levelFile);
						if (i < files.size() - 1
								&& levelFile.intersectsWith(files.get(i + 1)))
							intersection.add(files.get(i + 1));

						if (intersection.size() > 1) {

							clean = false;

							current.addAll(intersection);

							futures.add(execRec.submit(new Callable<Void>() {

								@Override
								public Void call() throws Exception {
									System.out
											.println("Recovering failed merge among "
													+ intersection.size()
													+ " files. ");
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

}
