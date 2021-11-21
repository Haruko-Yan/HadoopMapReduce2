package group.s3749857.Assignment2Task3;

import org.apache.commons.math3.util.FastMath;

public class EuclidianDistance {
	
	/**
	 * Calculate the distance between medoid and data point.
	 * @param medoid
	 * @param dataPoint
	 * @return
	 */
	public static double measureDistance(Medoid medoid, DataPoint dataPoint) {
		double sum = 0;
		double[] point1 = medoid.getCenter().toArray();
		double[] point2 = dataPoint.getVector().toArray();
		
		for (int i = 0; i < point1.length; i++) {
			double diff = point1[i] - point2[i];
			sum += (diff * diff);
		}
		return FastMath.sqrt(sum);
	}
	
	/**
	 * Calculate the distance between data point as medoid and data point.
	 * @param medoid
	 * @param dataPoint
	 * @return
	 */
	public static double measureDistance(DataPoint asMedoid, DataPoint dataPoint) {
		double sum = 0;
		double[] point1 = asMedoid.getVector().toArray();
		double[] point2 = dataPoint.getVector().toArray();
		
		for (int i = 0; i < point1.length; i++) {
			double diff = point1[i] - point2[i];
			sum += (diff * diff);
		}
		return FastMath.sqrt(sum);
	}

}
