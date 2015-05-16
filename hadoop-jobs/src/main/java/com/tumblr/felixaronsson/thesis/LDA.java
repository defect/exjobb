package com.tumblr.felixaronsson.thesis;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.mahout.clustering.lda.cvb.CVB0Driver;

public class LDA{
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		int num = Integer.parseInt(args[0]);
		
		String inputPath = "fa-tumblr-thesis/output/tf_matrix/matrix/";
		String dictionaryPath = "fa-tumblr-thesis/output/dictionary";
		String topicModelPath = "fa-tumblr-thesis/output/topic_model/";
		String docTopicPath = "fa-tumblr-thesis/output/doc_topic/";
		String modelStateTempPath = "fa-tumblr-thesis/temp/";
		
		JobConf conf = new JobConf(LDA.class);
		conf.setJobName("cvb0");

		CVB0Driver cvb = new CVB0Driver();
		
		cvb.run(conf, new Path(inputPath), new Path(topicModelPath), num, 100784, 
				0.0001, 0.0001, 15, 1, 0.05, new Path(dictionaryPath), new Path(docTopicPath),
				new Path(modelStateTempPath), System.nanoTime() % 10000, (float)0.05, 4, 1, 10, 10, false);
	}
}
