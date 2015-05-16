package com.tumblr.felixaronsson.thesis;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/* 
 * Input format: 
 * - Last.fm data: UUID<sep>Object name<sep>Tag name<sep>Tag count
 * - Tumblr data:  Blog ID \t Unigram \t Unigram count \t Unigram type (desc, tag, et.c.) 
 * 
 * Output key: 		Text - "Object name"
 * Output value: 	IntWritable - 1
 * 
 */
public class UniqueTags extends Configured implements Tool {
	
	enum ParseFailures {
		NOT_A_TAG,
		EMPTY_TAG,
		SKIPPED,
		STOPWORD
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
		Job job = new Job(conf, "fa-mahout-thesis-unique-tags");
		job.setJarByClass(getClass());
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(conf.get("input")));
		SequenceFileOutputFormat.setOutputPath(job, new Path(conf.get("output")));
		
		if (conf.get("dataset").equals("lastfm")) {
			job.setMapperClass(LastFMUniqueTagsMapper.class);
		}
		else if (conf.get("dataset").equals("tumblr")) {
			job.setMapperClass(TumblrUniqueTagsMapper.class);
		}
		else {
			throw new IllegalArgumentException();
		}
		
		job.setReducerClass(UniqueTagsReducer.class);
			
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new UniqueTags(), args);
        System.exit(res);
	}
}
