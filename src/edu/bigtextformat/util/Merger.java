package edu.bigtextformat.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import edu.bigtextformat.block.Block;
import edu.bigtextformat.block.BlockFile;
import edu.bigtextformat.block.BlockFileOptions;
import edu.bigtextformat.levels.SortedLevelFile;
import edu.bigtextformat.manifest.Manifest;
import edu.jlime.util.compression.CompressionType;

public class Merger {
	public static void main(String[] args) throws Exception {
		String from = args[0];
		String to = args[1];

		merge(from, to);
	}

	public static void merge(String from, String to) throws Exception,
			IOException {
		File toFile = new File(to);
		File fromFile = new File(from);
		for (File sstFile : fromFile.listFiles()) {
			if (sstFile.getName().equals(".lock"))
				continue;
			else if (sstFile.getName().equals("MANIFEST.log")) {
				BlockFile fromManifest = BlockFile.open(sstFile.getPath(),
						Manifest.MAGIC);
				BlockFile toManifest = BlockFile.create(toFile.getPath()
						+ "/MANIFEST.log",
						new BlockFileOptions().setMagic(Manifest.MAGIC)
								.setEnableCache(false).setAppendOnly(true)
								.setComp(CompressionType.SNAPPY.getComp()),
						null);

				for (Block block : fromManifest) {
					toManifest.newBlock(block.payload(), true);
				}
				toManifest.close();
				fromManifest.close();
			} else if (!sstFile.getName().endsWith(".old")) {
				Path source = Paths.get(sstFile.getPath());

				String level = sstFile.getName().substring(0,
						sstFile.getName().indexOf("-"));
				Integer cont = Integer.valueOf(sstFile.getName().substring(
						sstFile.getName().indexOf("-") + 1,
						sstFile.getName().indexOf(".")));
				boolean done = false;
				while (!done) {
					String sstInDest = toFile.getPath() + "/" + level + "-"
							+ cont + ".sst";
					Path path = Paths.get(sstInDest);
					if (!Files.exists(path)) {
						Files.move(source, path);
						done = true;
					} else
						cont++;
				}
			}
		}

		SortedLevelFile f = SortedLevelFile.open(to, null);
		f.compact();
		f.close();
	}
}
