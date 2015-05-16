package com.tumblr.felixaronsson.thesis;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.distance.CosineDistanceMeasure;

public class SphericalKMeans extends Configured implements Tool {
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = getConf();

		String vectorsPath = conf.get("input");
		String initialCentroidsPath = conf.get("initial");
		String clusterPath = conf.get("clusters");
		
		int k = conf.getInt("k", 50);

		Path centroids = RandomSeedGenerator.buildRandom(conf, new Path(vectorsPath), new Path(initialCentroidsPath), k, new CosineDistanceMeasure());
		//Path centroids = new Path(initialCentroidsPath);
	    //KMeansDriver.run(conf, new Path(vectorsPath), centroids, new Path(clusterPath), new CosineDistanceMeasure(), 0.1, 100, true, 0, false);
	    
	    return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new SphericalKMeans(), args);
        System.exit(res);
	}
}