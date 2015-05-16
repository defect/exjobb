package com.tumblr.felixaronsson.thesis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.tumblr.felixaronsson.thesis.UniqueTags.ParseFailures;


/*
 * Input key: 		Text - "Object name"
 * Input value: 	IntWritable - 1
 * 
 * Output key: 		Text - "Object name"
 * Output value: 	IntWritable - 1
 */
public class UniqueTagsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	private double percentage;
	private int min_occurrences;
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {			
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		if(Math.random() < percentage && sum > min_occurrences) {
			context.write(key, new IntWritable(sum));
		}
		else {
			context.getCounter(ParseFailures.SKIPPED).increment(1);
		}
	}
	
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		percentage = conf.getFloat("percentage", 100)/100.0;
		min_occurrences = conf.getInt("min_occurrences", 0);
	}
}