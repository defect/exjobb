package com.tumblr.felixaronsson.thesis;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/*
 * Input formats: 
 * - Last.fm data: UUID<sep>Object name<sep>Tag name<sep>Tag count
 * - Tumblr data:  Blog ID \t Unigram \t Unigram count \t Unigram type (desc, tag, et.c.)  
 * 
 * Output key:		Text - "Tag name;Object name"
 * Output value: 	Text - "Tag count for object;Total tag count for object"
 * 
 * In the scope of this thesis an object will either be an artist or a blog.
 * 
 */
public class TagCount extends Configured implements Tool {

	enum ParseFailures {
		NOT_A_TAG,
		EMPTY_TAG
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
		Job job = new Job(conf, "fa-mahout-thesis-tag-count");
		job.setJarByClass(getClass());
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(conf.get("input")));
		SequenceFileOutputFormat.setOutputPath(job, new Path(conf.get("output")));
		
		if (conf.get("dataset").equals("lastfm")) {
			job.setMapperClass(LastFMTagCountMapper.class);
		}
		else if (conf.get("dataset").equals("tumblr")) {
			job.setMapperClass(TumblrTagCountMapper.class);
		}
		else {
			throw new IllegalArgumentException();
		}
		job.setReducerClass(TagCountReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;	
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new TagCount(), args);
        System.exit(res);
      }
}