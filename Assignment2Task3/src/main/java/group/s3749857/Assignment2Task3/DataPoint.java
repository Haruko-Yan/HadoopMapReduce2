package group.s3749857.Assignment2Task3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import de.jungblut.math.DoubleVector;
import de.jungblut.math.dense.DenseDoubleVector;

public class DataPoint implements WritableComparable<DataPoint> {

	private DoubleVector vector;
	
	public DataPoint() {
		super();
	}
	
	public DataPoint(DataPoint dataPoint) {
		this.vector = dataPoint.getVector().deepCopy();
	}
	
	public DataPoint(DenseDoubleVector vector) {
		this.vector = vector.deepCopy();
	}
	
	public DataPoint(double x, double y) {
		this.vector = new DenseDoubleVector(new double[] {x, y});
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(vector.getLength());
		for (int i = 0; i < vector.getDimension(); i++) {
			out.writeDouble(vector.get(i));
		}
	}
	public void readFields(DataInput in) throws IOException {
		final int length = in.readInt();
		DoubleVector vector = new DenseDoubleVector(length);
		for (int i = 0; i < length; i++) {
			vector.set(i, in.readDouble());
		}
		this.vector = vector;
	}

	public int compareTo(DataPoint o) {
		DoubleVector subtract = vector.subtract(o.getVector());
		if (subtract.sum() < 0) return -1;
		else if (subtract.sum() > 0) return 1;
		return 0;
	}
	
	@Override
	public String toString() {
		double[] array = vector.toArray();
		return "Data point: (" + array[0] + "," + array[1] + ")";
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DataPoint other = (DataPoint) obj;
		if (vector == null) {
			return other.vector == null;
		} else return vector.equals(other.vector);
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((vector == null) ? 0 : vector.hashCode());
		return result;
	}
	
	public DoubleVector getVector() {
		return vector;
	}

	public void setVector(DoubleVector vector) {
		this.vector = vector;
	}

}
