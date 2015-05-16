package com.tumblr.felixaronsson.thesis;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.util.GenericsUtil;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.VectorWritable;

public class LastVectors extends Configured implements Tool {
	enum Skipped {
		ARTIST,
		TAG
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		conf.set("io.serializations",
				 "org.apache.hadoop.io.serializer.JavaSerialization,"
			   + "org.apache.hadoop.io.serializer.WritableSerialization");

		Map<String,Integer> dictionary = new HashMap<String,Integer>();

		Path inputDir = new Path(conf.get("dict_input"));
		FileSystem fs = FileSystem.get(inputDir.toUri(), conf);
		FileStatus[] outputFiles = fs.globStatus(new Path(inputDir, "part-*"));

		int i = 0;
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, new Path(conf.get("dict_output")), Text.class, IntWritable.class);
		for (FileStatus fileStatus : outputFiles) {
			Path path = fileStatus.getPath();
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
			
			Text key = new Text();
			IntWritable value = new IntWritable();

			while (reader.next(key, value)) {
				dictionary.put(key.toString(), Integer.valueOf(i++));
				writer.append(key, new IntWritable(i-1));
			}
			reader.close();
		}
		writer.close();
		
		DefaultStringifier<Map<String, Integer>> mapStringifier = new DefaultStringifier<Map<String,Integer>>(conf, GenericsUtil.getClass(dictionary));
		conf.set("tag_dict", mapStringifier.toString(dictionary));
		
		
		Job job = new Job(conf, "tfidf vectors");
		job.setJarByClass(LastVectors.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VectorWritable.class);
		
	    SequenceFileInputFormat.addInputPath(job, new Path(conf.get("input")));
	    FileOutputFormat.setOutputPath(job, new Path(conf.get("output")));
	    
	    job.setMapperClass(LastVectorsMapper.class);
	    job.setReducerClass(LastVectorsReducer.class);

	    return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new LastVectors(), args);
        System.exit(res);
      }
}
