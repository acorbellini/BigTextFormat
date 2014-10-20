package edu.bigtextformat.raw;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class RawFileChannel extends RawFile {

	private FileChannel f;

	public RawFileChannel(String path, boolean memoryMapped) throws IOException {
		Path p = Paths.get(path);
		this.f = FileChannel.open(p, StandardOpenOption.CREATE);
	}

	@Override
	public long length() throws Exception {
		return f.size();
	}

	@Override
	public void write(long pos, byte[] byteArray) throws Exception {
		f.write(ByteBuffer.wrap(byteArray), pos);
	}

	@Override
	public void writeByte(long l, byte b) throws Exception {
		f.write(ByteBuffer.wrap(new byte[] { b }), l);
	}

	@Override
	public void read(long pos, byte[] data, int offset, int size)
			throws Exception {
		ByteBuffer buff = ByteBuffer.wrap(data, offset, size);
		f.read(buff, pos);
	}

}
