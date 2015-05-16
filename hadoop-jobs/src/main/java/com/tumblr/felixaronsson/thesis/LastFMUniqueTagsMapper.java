package com.tumblr.felixaronsson.thesis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Input key: 		LongWritable - line number
 * Input value: 	Text - line of file
 * 
 * Output key: 		Text - "Artist name"
 * Output value: 	IntWritable - 1
 */
public class LastFMUniqueTagsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	public void map(LongWritable line, Text data, Context context) throws IOException, InterruptedException {
		String[] fields = data.toString().split("<sep>");
		String tagname = fields[2];
		context.write(new Text(tagname), new IntWritable(1));
	}
}