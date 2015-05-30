package edu.bigtextformat.levels.memtable;


public class Operation
// implements DataType<Operation> 
{
	OperationType op;
	byte[] k = new byte[] {};
	byte[] v = new byte[] {};

	public Operation() {
	}

	public Operation(OperationType opID, byte[] k, byte[] v) {
		super();
		this.op = opID;
		this.k = k;
		this.v = v;
	}

	// @Override
	// public Operation fromByteArray(byte[] data) throws Exception {
	// ByteBuffer buff = new ByteBuffer(data);
	// this.op = OperationType.fromID(buff.get());
	// this.k = buff.getByteArray();
	// this.v = buff.getByteArray();
	// return this;
	// }
	//
	// public byte[] toByteArray() {
	// ByteBuffer buff = new ByteBuffer();
	// buff.put(op.getId());
	// buff.putByteArray(k);
	// buff.putByteArray(v);
	// return buff.build();
	// }

}
