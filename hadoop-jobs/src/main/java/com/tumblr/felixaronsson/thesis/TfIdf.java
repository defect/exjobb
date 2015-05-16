package com.tumblr.felixaronsson.thesis;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class TfIdf extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf, "fa-mahout-thesis-tfidf");
		job.setJarByClass(getClass());
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		SequenceFileInputFormat.addInputPath(job, new Path(conf.get("input")));
		SequenceFileOutputFormat.setOutputPath(job, new Path(conf.get("output")));
		
		job.setMapperClass(TfIdfMapper.class);
		job.setReducerClass(TfIdfReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new TfIdf(), args);
        System.exit(res);
	}
}
