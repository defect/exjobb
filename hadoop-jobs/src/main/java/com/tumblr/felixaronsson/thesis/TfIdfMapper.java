package com.tumblr.felixaronsson.thesis;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TfIdfMapper extends Mapper<Text, Text, Text, Text> {
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		String[] kstrings = key.toString().split(";");
		String[] vstrings = value.toString().split(";");
		//                      <Tag name>             <Object name>  ;    <Tag count for object>;    <Total tag count for object>
		context.write(new Text(kstrings[0]), new Text(kstrings[1] + ";" + vstrings[0] + ";" + vstrings[1]));
	}
}