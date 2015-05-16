package com.tumblr.felixaronsson.thesis;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.common.distance.TanimotoDistanceMeasure;


public class CanopySeededSphericalKMeans{
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		double t1 = Double.parseDouble(args[0]); 
		double t2 = Double.parseDouble(args[1]);
		
		String vectorsPath = "/fa-mahout-thesis/input/LastFMSparseVectors100";
		String initialCentroidsPath = "/fa-mahout-thesis/output/initialcanopy/";
		String clusterPath = "/fa-mahout-thesis/output/cluster/";
		
		JobConf conf = new JobConf(CanopySeededSphericalKMeans.class);
		conf.setJobName("spherical-kmeans");

		CanopyDriver.run(conf, new Path(vectorsPath), new Path(initialCentroidsPath), new TanimotoDistanceMeasure(), t1, t2, true, 0, false);
	    //KMeansDriver.run(conf, new Path(vectorsPath), new Path(initialCentroidsPath), new Path(clusterPath), new CosineDistanceMeasure(), 0.1, 100, true, 0, false);
	}
}