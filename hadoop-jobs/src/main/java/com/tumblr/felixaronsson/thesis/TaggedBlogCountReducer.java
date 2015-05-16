package com.tumblr.felixaronsson.thesis;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;


/*
 * Input key:		Text - "Object name"
 * Input value: 	LongWritable - 1
 * 
 * Output key:		Text - "Object name"
 * Output value:	LongWritable - 1
 */
public class TaggedBlogCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		long sum = 0;
		for(LongWritable v : values) {
			long val = v.get();
			sum += val;
		}
		context.write(key, new LongWritable(sum));
	}
}