package edu.bigtextformat.data;

import java.util.Arrays;
import java.util.Iterator;

import edu.bigtextformat.Data;
import edu.bigtextformat.Range;
import edu.bigtextformat.block.Block;
import edu.bigtextformat.block.BlockFile;
import edu.bigtextformat.block.BlockFormat;
import edu.bigtextformat.index.BplusIndex;
import edu.bigtextformat.index.Index;
import edu.bigtextformat.record.FormatType;
import edu.bigtextformat.record.FormatTypes;
import edu.bigtextformat.record.RecordFormat;
import edu.jlime.util.DataTypeUtils;

public class DataImpl implements Data {
	private static final int DEFAULT_BLOCK_SIZE = 512;
	private static final int DEFAULT_MAX_BLOCK_SIZE = 1024;
	BlockFile file;
	Index dataIndex;
	Index deleted; // En el caso de los datos, es probable que sea mas
	// util tener un indice para los borrados, ya que es mas dificil compactar
	String filePath;
	private int blockSize = DEFAULT_BLOCK_SIZE;
	private int maxBlockSize = DEFAULT_MAX_BLOCK_SIZE;
	private int headerSize = 128;
	private static long magic = DataTypeUtils.byteArrayToLong(new byte[] { 0xf,
			0xa, 0xc, 0xa, 0x1, 0x2, 0x3, 0x4 });

	private BlockFormat format;

	// Los bloques de datos se parten cuando se pasan de cierto tamaño.

	public DataImpl(String filePath) throws Exception {
		file = BlockFile.open(filePath, headerSize, blockSize, magic, true);
		String type = file.getHeader().getString("format_type");
		byte[] bs = file.getHeader().get("format");
		format = BlockFormat.getFormat(type, bs);

		BlockFormat indexformat = format.getKeyFormat();

		dataIndex = new BplusIndex(filePath + ".i", indexformat);

		deleted = new BplusIndex(filePath + ".del",
				new RecordFormat(Arrays.asList(new String[] { "recordpos" }),
						Arrays.asList(new FormatType<?>[] { FormatTypes.LONG
								.getType() }),
						Arrays.asList(new String[] { "recordpos" })));
	}

	public void addData(BlockData data) throws Exception {
		byte[] k = format.getKey(data);
		long pos = dataIndex.getBlockPosition(k);
		Block b = file.getBlock(pos, true);

		DataPayload dataPayload = new DataPayload().fromByteArray(b.payload());

		dataPayload.add(data.toByteArray());

		if (dataPayload.size() > maxBlockSize) {
			Range orig = dataPayload.range();
			DataPayload splitted = dataPayload.split();
			if (splitted != null) {
				Block halfBlock = file.newBlock(splitted.toByteArray());
				dataIndex.splitRange(orig, dataPayload.range(),
						splitted.range(),
						DataTypeUtils.longToByteArray(halfBlock.getPos()));
			}

		}
		Block writtenBlock = file.newBlock(dataPayload.toByteArray());
		dataIndex.put(k, DataTypeUtils.longToByteArray(writtenBlock.getPos()));
	}

	// public Iterator<Block> iterator(byte[] from, byte[] to) {
	// Iterator<Long> pos = dataIndex.iterator(new Range(from, to));
	// return file.iterator(pos);
	// }
}
