package edu.bigtextformat.levels;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import edu.bigtextformat.block.Block;
import edu.bigtextformat.block.BlockFile;
import edu.bigtextformat.block.BlockFileOptions;
import edu.bigtextformat.levels.levelfile.LevelFile;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.DataTypeUtils;
import edu.jlime.util.compression.CompressionType;

public class Manifest {

	private static final long MAGIC = DataTypeUtils.byteArrayToLong("MANIFEST"
			.getBytes());
	BlockFile log;
	HashSet<String> added = new HashSet<>();
	HashMap<String, LevelFile> files = new HashMap<>();

	int maxCont = 0;

	long id = System.nanoTime();

	private String path;

	private File fDir;

	public Manifest(File fDir) throws Exception {
		this.path = fDir.getPath() + "/MANIFEST.log";
		this.fDir = fDir;
		try {
			log = BlockFile.open(path, MAGIC);

		} catch (Exception e) {
		}
		if (log == null || log.isEmpty()) {
			if (log != null && log.isEmpty())
				log.delete();
			log = createLog(path);
			checkOtherFiles(log);
		} else {
			log = compact(log, true);
		}

	}

	private BlockFile createLog(String path) throws Exception {
		return BlockFile.create(
				path,
				new BlockFileOptions().setMagic(MAGIC).setEnableCache(false)
						.setAppendOnly(true)
						.setComp(CompressionType.SNAPPY.getComp()));
	}

	public synchronized BlockFile compact(BlockFile oldLog, boolean addFiles)
			throws Exception, IOException {
		String newPath = fDir.getPath() + "/MANIFEST.log.new";
		BlockFile current = createLog(newPath);
		try {

			for (Block block : oldLog) {
				ByteBuffer buff = new ByteBuffer(block.payload());
				int level = buff.getInt();
				int cont = buff.getInt();
				String levelFileName = buff.getString();
				byte[] minKey = buff.getByteArray();
				byte[] maxKey = buff.getByteArray();

				if (Files
						.exists(Paths.get(fDir.getPath() + "/" + levelFileName))) {
					if (cont > maxCont)
						maxCont = cont;
					if (!files.containsKey(levelFileName)) {
						files.put(
								levelFileName,
								LevelFile.open(level, cont, fDir.getPath()
										+ "/" + levelFileName, minKey, maxKey));
						put(current, level, cont, levelFileName, minKey, maxKey);
					} else {
						System.out.println("Was already there.");
					}

				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		checkOtherFiles(current);

		try {
			oldLog.delete();
			current.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		Files.move(Paths.get(newPath), Paths.get(path));
		return createLog(path);
	}

	private void checkOtherFiles(BlockFile current) throws Exception {
		for (File currentFiles : fDir.listFiles()) {
			String name = currentFiles.getName();
			if (name.endsWith(".sst") && !files.containsKey(name)) {
				Integer level = Integer.valueOf(name.substring(0,
						name.indexOf("-")));
				Integer cont = Integer.valueOf(name.substring(
						name.indexOf("-") + 1, name.indexOf(".")));

				try {
					LevelFile open = LevelFile.open(level, cont, fDir.getPath()
							+ "/" + name, null, null);
					files.put(currentFiles.getName(), open);
					put(current, level, cont, name, open.getMinKey(),
							open.getMaxKey());
				} catch (Exception e) {
					e.printStackTrace();
					files.remove(currentFiles.getName());
				}
			}
		}
	}

	public synchronized void compact() throws Exception {
		log.close();
		added.clear();
		log = BlockFile.open(path, MAGIC);
		log = compact(log, false);
	}

	public int getMaxCount() {
		return maxCont;
	}

	public LevelFile getFirstLevelFile() {
		return files.get(0);
	}

	public Collection<LevelFile> readLevelFiles() {
		Collection<LevelFile> ret = files.values();
		files = new HashMap<>();
		// added.clear();
		return ret;
	}

	public void put(BlockFile current, int level, int cont,
			String levelFileName, byte[] minKey, byte[] maxKey)
			throws Exception {
		if (added.contains(levelFileName))
			return;
		added.add(levelFileName);
		ByteBuffer buff = new ByteBuffer();
		buff.putInt(level);
		buff.putInt(cont);
		buff.putString(levelFileName);
		buff.putByteArray(minKey);
		buff.putByteArray(maxKey);
		synchronized (this) {
			current.newFixedBlock(buff.build());
		}
	}

	public void put(int level, int cont, String levelFileName, byte[] minKey,
			byte[] maxKey) throws Exception {
		put(log, level, cont, levelFileName, minKey, maxKey);
	}

	public void close() throws IOException {
		log.close();
	}

}
