package com.tumblr.felixaronsson.thesis;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BuildDictionary extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		Path inputDir = new Path(conf.get("input"));
		FileSystem fs = FileSystem.get(inputDir.toUri(), conf);
		FileStatus[] outputFiles = fs.globStatus(new Path(inputDir, "part-*"));

		int i = 0;
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, new Path(conf.get("output")), Text.class, IntWritable.class);
		for (FileStatus fileStatus : outputFiles) {
			Path path = fileStatus.getPath();
			System.out.println("Reading: " + path.toString());
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
			
			Text key = new Text();
			IntWritable value = new IntWritable();

			while (reader.next(key, value)) {
				writer.append(key, new IntWritable(i));
				i++;
			}
			reader.close();
		}
		writer.close();

		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new BuildDictionary(), args);
        System.exit(res);
      }
}
