package edu.bigtextformat.levels.compactor;

public interface Compactor {

	public abstract void waitFinished();

	public abstract void start();

	public abstract void setChanged();

	public abstract void compact(int level);

	public abstract void forcecompact() throws Exception;

}