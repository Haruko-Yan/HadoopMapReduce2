package group.s3749857.Assignment2Task3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import de.jungblut.math.DoubleVector;
import de.jungblut.math.dense.DenseDoubleVector;

public class Medoid implements WritableComparable<Medoid>{

	private DoubleVector center;
	private double cost;
	
	public Medoid() {
		super();
	}
	
	public Medoid(Medoid medoid) {
		this.center = medoid.center.deepCopy();
		this.cost = medoid.cost;
	}
	
	public Medoid(DoubleVector center, double cost) {
		this.center = center;
		this.cost = cost;
	}
	
	public Medoid(DataPoint center, double cost) {
		this.center = center.getVector();
		this.cost = cost;
	}
	
	public Medoid(double x, double y, double cost) {
		this.center = new DenseDoubleVector(new double[] {x, y});
		this.cost = cost;
	}

	public void write(DataOutput out) throws IOException {
		out.writeDouble(cost);
		out.writeInt(center.getLength());
		for (int i = 0; i < center.getDimension(); i++) {
			out.writeDouble(center.get(i));
		}
	}

	public void readFields(DataInput in) throws IOException {
		cost = in.readDouble();
		final int length = in.readInt();
		DoubleVector vector = new DenseDoubleVector(length);
		for (int i = 0; i < length; i++) {
			vector.set(i, in.readDouble());
		}
		center = vector;
	}
	
	public int compareTo(Medoid o) {
		DoubleVector subtract = center.subtract(o.getCenter());
		if (subtract.sum() < 0) return -1;
		else if (subtract.sum() > 0) return 1;
		return 0;
	}
	
	@Override
	public String toString() {
		double[] array = center.toArray();
		return "Medoid: (" + array[0] + "," + array[1] + ")";
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Medoid other = (Medoid) obj;
		if (center == null) {
			return other.center == null;
		} else return center.equals(other.center);
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((center == null) ? 0 : center.hashCode());
		return result;
	}
	
	public DoubleVector getCenter() {
		return center;
	}

	public void setCenter(DoubleVector center) {
		this.center = center;
	}

	public double getCost() {
		return cost;
	}

	public void setCost(double cost) {
		this.cost = cost;
	}
	
}
