package com.tumblr.felixaronsson.thesis;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;

public class KMeans{
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		JobConf conf = new JobConf(KMeans.class);
		conf.setJobName("fa-tumblr-thesis-kmeans");

		
		/*
		String vectorsPath = "s3n://fa-mahout-thesis/input/";
		String initialCentroidsPath = "s3n://fa-mahout-thesis/output/initial/";
		String clusterPath = "s3n://fa-mahout-thesis/output/cluster/";
		*/
		
		String vectorsPath = conf.get("input");
		String initialCentroidsPath = conf.get("initial");
		String clusterPath = conf.get("clusters");
		
		int k = conf.getInt("k", 50);
		
		Path centroids = RandomSeedGenerator.buildRandom(conf, new Path(vectorsPath), new Path(initialCentroidsPath), k, new EuclideanDistanceMeasure());
	    KMeansDriver.run(conf, new Path(vectorsPath), centroids, new Path(clusterPath), new EuclideanDistanceMeasure(), 0.5, 100, true, 0, false);
	}
}
