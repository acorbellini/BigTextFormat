package edu.bigtextformat;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import edu.bigtextformat.block.Block;
import edu.bigtextformat.block.BlockFile;
import edu.jlime.util.DataTypeUtils;
import edu.jlime.util.compression.Compression.CompressionType;

public class BlockFileTest {
	public static void main(String[] args) throws Exception {
		new BlockFileTest().test();
	}

	final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();

	public static String bytesToHex(byte[] bytes) {
		char[] hexChars = new char[bytes.length * 2];
		for (int j = 0; j < bytes.length; j++) {
			int v = bytes[j] & 0xFF;
			hexChars[j * 2] = hexArray[v >>> 4];
			hexChars[j * 2 + 1] = hexArray[v & 0x0F];
		}
		return new String(hexChars);
	}

	private void test() throws Exception {
		long magic = DataTypeUtils.byteArrayToLong("ABLKFILE".getBytes());
		final BlockFile file = BlockFile.create("blocktest.b", 100, 16, magic,
				CompressionType.SNAPPY.getComp(), true);

		String[] test = new String[] { "Chorioactis is a genus of fungus that contains the single species Chorioactis geaster, an extremely rare mushroom found only  "
				+ "in select locales in Texas and Japan. In the former, it is commonly known as the devil's cigar or the Texas star in Japan it is called kirinomitake. It is notable for its unusual appearance. "
				+ "The fruit body, which grows on the stumps or dead roots of cedar elms "
				+ "(in Texas) or dead oaks (in Japan), somewhat resembles a dark brown or black cigar before it splits open radially into a starlike arrangement of four"
				+ " to seven leathery rays. The interior surface of the fruit body bears the spore-bearing"
				+ " tissue, and is colored white to brown, depending on its age. Fruit body opening can be"
				+ " accompanied by a distinct hissing sound and the release of a smoky cloud of spores. Fruit bodies were first collected in Austin, Texas, and the species was named Urnula geaster "
				+ "in 1893; it was later found in Kyushu in 1937, but the mushroom was not reported again in Japan "
				+ "until 1973. Although the new genus Chorioactis was proposed to accommodate the unique species "
				+ "a few years after its original discovery, it was not until 1968 that it was accepted as a "
				+ "valid genus" };
		long sizeOrig = 0;
		if (file.isEmpty()) {
			// for (String string : test) {
			// byte[] b = string.getBytes();
			// sizeOrig += b.length;
			// file.newBlock(b);
			// }
			// System.out.println(file.length() * 100 / (float) sizeOrig);

			ExecutorService exec = Executors.newFixedThreadPool(10);
			for (int i = 0; i < 500; i++) {
				final int toPrint = i;
				exec.execute(new Runnable() {
					@Override
					public void run() {
						try {
							Block b = file.newBlock(Integer.valueOf(toPrint)
									.toString().getBytes());
							b.setPayload((toPrint + " Todo cambiado!!! 0123456789ABCDEF jeje")
									.getBytes());
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				});
			}
			exec.shutdown();
			exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
			// Block b1 = file.newBlock("Hola!".getBytes());
			// b1.setPayload("Hola! que tal? como va? todo bien?".getBytes());
		} else {
			System.out.println("Is not empty");

			for (Block b : file) {
				if (b.isDeleted())
					System.out.print("DELETED : ");
				System.out.println(new String(b.payload()));
				// System.out.println(DataTypeUtils.byteArrayToInt(b.payload()));
			}
		}
		// long oldPos = b1.getPos();
		// b1.setPayload("hola como vas? todo bien????".getBytes());
		// long newPos = b1.getPos();
		//
		// System.out.println(new String(file.getBlock(oldPos).payload()));
		// System.out.println(new String(file.getBlock(newPos).payload()));
		file.close();

	}
}
