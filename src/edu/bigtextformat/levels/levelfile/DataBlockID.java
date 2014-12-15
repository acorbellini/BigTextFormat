package edu.bigtextformat.levels.levelfile;

import java.util.UUID;

public class DataBlockID {
	public static DataBlockID create(UUID id, long pos) {
		return new DataBlockID(id, pos);
	}
	UUID fileID;

	long pos;

	public DataBlockID(UUID fileID, long pos) {
		super();
		this.fileID = fileID;
		this.pos = pos;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DataBlockID other = (DataBlockID) obj;
		if (fileID == null) {
			if (other.fileID != null)
				return false;
		} else if (!fileID.equals(other.fileID))
			return false;
		if (pos != other.pos)
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((fileID == null) ? 0 : fileID.hashCode());
		result = prime * result + (int) (pos ^ (pos >>> 32));
		return result;
	}

}