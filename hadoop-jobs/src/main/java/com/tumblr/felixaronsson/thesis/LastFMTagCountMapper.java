package com.tumblr.felixaronsson.thesis;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Input key: 		LongWritable - line number
 * Input value: 	Text - line of file
 * 
 * Output key: 		Text - "Artist name"
 * Output value: 	Text - "Tag name;Tag count"
 */
public class LastFMTagCountMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable line, Text data, Context context) throws IOException, InterruptedException {
		String[] strings = data.toString().split("<sep>");
		context.write(new Text(strings[1]), new Text(strings[2] + ";" + strings[3]));
	}
}