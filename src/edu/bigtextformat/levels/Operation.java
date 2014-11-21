package edu.bigtextformat.levels;

import edu.bigtextformat.record.DataType;
import edu.jlime.util.ByteBuffer;

public class Operation implements DataType<Operation> {
	byte opID;
	byte[] k = new byte[] {};
	byte[] v = new byte[] {};

	public Operation() {
	}

	public Operation(byte opID, byte[] k, byte[] v) {
		super();
		this.opID = opID;
		this.k = k;
		this.v = v;
	}

	public byte[] toByteArray() {
		ByteBuffer buff = new ByteBuffer();
		buff.put(opID);
		buff.putByteArray(k);
		buff.putByteArray(v);
		return buff.build();
	}

	@Override
	public Operation fromByteArray(byte[] data) throws Exception {
		ByteBuffer buff = new ByteBuffer(data);
		this.opID = buff.get();
		this.k = buff.getByteArray();
		this.v = buff.getByteArray();
		return this;
	}

}
