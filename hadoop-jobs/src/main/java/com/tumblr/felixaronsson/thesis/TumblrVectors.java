package com.tumblr.felixaronsson.thesis;

import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.VectorWritable;
import org.apache.hadoop.filecache.DistributedCache;

public class TumblrVectors extends Configured implements Tool {
	enum Skipped {
		ARTIST,
		TAG
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
		Job job = new Job(conf, "tfidf vectors");
		job.setJarByClass(TumblrVectors.class);
			
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VectorWritable.class);
		
		DistributedCache.addCacheFile(new URI(conf.get("dictionary")), job.getConfiguration());
		
	    SequenceFileInputFormat.addInputPath(job, new Path(conf.get("input")));
	    FileOutputFormat.setOutputPath(job, new Path(conf.get("output")));
	    
	    job.setMapperClass(TumblrVectorsMapper.class);
	    job.setReducerClass(TumblrVectorsReducer.class);

	    return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new TumblrVectors(), args);
        System.exit(res);
      }
}
