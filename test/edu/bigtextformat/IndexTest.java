package edu.bigtextformat;

import edu.bigtextformat.index.BplusIndex;
import edu.bigtextformat.index.IndexData;
import edu.bigtextformat.record.FormatType;
import edu.bigtextformat.record.FormatTypes;
import edu.bigtextformat.record.RecordFormat;

public class IndexTest {
	public static void main(String[] args) throws Exception {
		RecordFormat format = RecordFormat.create(new String[] { "k" },
				new FormatType<?>[] { FormatTypes.STRING.getType() },
				new String[] { "k" });

		BplusIndex i = new BplusIndex("indextest.bplus", format);

		byte[] k1 = format.newRecord().set("k", "Primero").toByteArray();
		byte[] k2 = format.newRecord().set("k", "Segundo").toByteArray();
		byte[] k3 = format.newRecord().set("k", "Tercero").toByteArray();
		byte[] k4 = format.newRecord().set("k", "Cuarto").toByteArray();
		byte[] k5 = format.newRecord().set("k", "Quinto").toByteArray();

		i.put(k1, "BLKSTART".getBytes());
		i.put(k2, "BLKSTART".getBytes());
		i.put(k3, "BLKSTART".getBytes());
		i.put(k4, "BLKSTART".getBytes());
		i.put(k5, "BLKSTART".getBytes());

		for (IndexData data : i) {
			for (byte[] b : data.getValues()) {
				System.out.println(format);
			}
		}

		System.out.println(new String(i.get(k1)));
		System.out.println(new String(i.get(k2)));
		System.out.println(new String(i.get(k3)));
		System.out.println(new String(i.get(k4)));
		System.out.println(new String(i.get(k5)));
	}
}
