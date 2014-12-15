package edu.bigtextformat.raw;

public class FileChannelException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1460312031253859464L;

	public FileChannelException(RawFile rawFileChannel, Exception e) {
		super("Exception on raw file " + rawFileChannel.getPath(), e);
	}

}
