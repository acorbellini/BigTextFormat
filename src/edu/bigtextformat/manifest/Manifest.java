package edu.bigtextformat.manifest;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import edu.bigtextformat.block.Block;
import edu.bigtextformat.block.BlockFile;
import edu.bigtextformat.block.BlockFileOptions;
import edu.bigtextformat.levels.SortedLevelFile;
import edu.bigtextformat.levels.levelfile.LevelFile;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.DataTypeUtils;
import edu.jlime.util.compression.CompressionType;

public class Manifest {

	public static final long MAGIC = DataTypeUtils.byteArrayToLong("MANIFEST"
			.getBytes());
	private static final int READ = 0;
	private static final int APPEND = 1;
	private static final int COMPACT = 2;
	BlockFile log;
	Set<String> inFile = Collections
			.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

	long id = System.nanoTime();

	private String path;

	private File fDir;
	private int mode = -1;
	private SortedLevelFile sorted;

	public Manifest(File fDir, SortedLevelFile sll) throws Exception {
		this.sorted = sll;
		this.path = fDir.getPath() + "/MANIFEST.log";
		this.fDir = fDir;
		try {
			readMode();
		} catch (Exception e) {
		}
		if (log == null || log.isEmpty()) {
			if (log != null && log.isEmpty()) {
				log.close();
				log.delete();
			}
			Path old = Paths.get(fDir.getPath() + "/MANIFEST.log.old");
			if (Files.exists(old)) {
				Files.move(old, Paths.get(path),
						StandardCopyOption.REPLACE_EXISTING);
				readMode();
			} else {
				appendMode();
				Map<String, LevelFile> files = checkOtherFiles(new HashMap<String, LevelFile>());
				addAll(files, log);
			}
		}

	}

	private void addAll(Map<String, LevelFile> files, final BlockFile current) {

		for (final LevelFile lf : files.values()) {
			try {
				put(current, lf.getLevel(), lf.getCont(), lf.getName(),
						lf.getMinKey(), lf.getMaxKey());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private void appendMode() throws Exception {
		if (mode == APPEND)
			return;
		if (log != null)
			log.close();
		log = createLog(path);
		mode = APPEND;
	}

	private Map<String, LevelFile> checkOtherFiles(
			final HashMap<String, LevelFile> files) {
		ExecutorService exec = Executors.newFixedThreadPool(20,
				new ThreadFactory() {

					@Override
					public Thread newThread(Runnable r) {
						Thread t = Executors.defaultThreadFactory()
								.newThread(r);
						t.setName("Manifest missing file checker for " + fDir);
						return t;
					}
				});
		final Map<String, LevelFile> ret = new ConcurrentHashMap<>();
		for (final File currentFile : fDir.listFiles()) {
			exec.execute(new Runnable() {

				@Override
				public void run() {
					String name = currentFile.getName();
					if (name.endsWith(".sst") && !files.containsKey(name)) {
						Integer level = Integer.valueOf(name.substring(0,
								name.indexOf("-")));
						Integer cont = Integer.valueOf(name.substring(
								name.indexOf("-") + 1, name.indexOf(".")));

						try {
							LevelFile open = LevelFile.open(level, cont,
									fDir.getPath() + "/" + name, null, null,
									sorted);
							if (sorted.getOpts() != null)
								open.setOpts(sorted.getOpts());
							open.getMinKey();
							open.getMaxKey();
							ret.put(currentFile.getName(), open);
						} catch (Exception e) {
							e.printStackTrace();
							ret.remove(currentFile.getName());
							currentFile.delete();
						}
					}
				}
			});
		}
		exec.shutdown();
		try {
			exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return ret;
	}

	public void close() throws IOException {
		log.close();
	}

	public synchronized void compact(HashMap<String, LevelFile> files)
			throws Exception, IOException {

		String newPath = fDir.getPath() + "/MANIFEST.log.new";

		String backup = fDir.getPath() + "/MANIFEST.log.old";

		Files.copy(Paths.get(path), Paths.get(backup),
				StandardCopyOption.REPLACE_EXISTING);

		inFile.clear();

		BlockFile current = createLog(newPath);
		try {
			addAll(files, current);
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			log.delete();
			current.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		mode = COMPACT;
		Files.move(Paths.get(newPath), Paths.get(path));
		appendMode();
	}

	private BlockFile createLog(String path) throws Exception {
		return BlockFile.create(
				path,
				new BlockFileOptions().setMagic(MAGIC).setEnableCache(false)
						.setAppendOnly(true)
						.setComp(CompressionType.SNAPPY.getComp()), null);
	}

	public Collection<LevelFile> getFiles() throws IOException, Exception {
		HashMap<String, LevelFile> files = readFiles();

		Map<String, LevelFile> checkOtherFiles = checkOtherFiles(files);
		if (!checkOtherFiles.isEmpty()) {
			files.putAll(checkOtherFiles);
			compact(files);
		}

		return files.values();
	}

	public void put(BlockFile current, int level, int cont,
			String levelFileName, byte[] minKey, byte[] maxKey)
			throws Exception {
		if (inFile.contains(levelFileName))
			return;
		else
			synchronized (inFile) {
				if (inFile.contains(levelFileName))
					return;
				inFile.add(levelFileName);
			}

		ByteBuffer buff = new ByteBuffer();
		buff.putInt(level);
		buff.putInt(cont);
		buff.putString(levelFileName);
		buff.putByteArray(minKey);
		buff.putByteArray(maxKey);
		synchronized (this) {
			current.newBlock(buff.build(), true);
		}
	}

	public void put(int level, int cont, String levelFileName, byte[] minKey,
			byte[] maxKey) throws Exception {
		put(log, level, cont, levelFileName, minKey, maxKey);
	}

	public HashMap<String, LevelFile> readFiles() throws IOException, Exception {
		HashMap<String, LevelFile> files = new HashMap<String, LevelFile>(
				200000);

		inFile.clear();

		HashSet<String> inDir = new HashSet<String>();
		for (String string : fDir.list()) {
			inDir.add(string);
		}

		readMode();

		for (Block block : log) {
			ByteBuffer buff = new ByteBuffer(block.payload());
			int level = buff.getInt();
			int cont = buff.getInt();
			String levelFileName = buff.getString();
			byte[] minKey = buff.getByteArray();
			byte[] maxKey = buff.getByteArray();

			// if (Files.exists(Paths.get(fDir.getPath() + "/" +
			// levelFileName))) {
			if (inDir.contains(levelFileName)) {
				if (!files.containsKey(levelFileName)) {
					LevelFile open = LevelFile.open(level, cont, fDir.getPath()
							+ "/" + levelFileName, minKey, maxKey, sorted);
					open.setOpts(sorted.getOpts());
					files.put(levelFileName, open);
					inFile.add(levelFileName);
				} else {
					// System.out.println("Was already there.");
				}

			}
		}

		appendMode();

		return files;
	}

	private void readMode() throws IOException, Exception {
		if (mode == READ)
			return;
		if (log != null)
			log.close();
		log = BlockFile.open(path, MAGIC);
		mode = READ;
	}

}
