package edu.bigtextformat.raw;

public class FileChannelException extends Exception {

	public FileChannelException(RawFile rawFileChannel, Exception e) {
		super("Exception on raw file " + rawFileChannel.getPath(), e);
	}

}
