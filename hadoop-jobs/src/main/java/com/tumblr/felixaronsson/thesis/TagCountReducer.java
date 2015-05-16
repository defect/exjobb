package com.tumblr.felixaronsson.thesis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/*
 * Input key:		Text - "Object name"
 * Input value: 	Text - "Tag name;Tag count"
 * 
 * Output key:		Text - "Tag name;Object name"
 * Output value:	Text - "Tag count for object;Total tag count for object"
 */
public class TagCountReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		ArrayList<String> cache = new ArrayList<String>();
		long sum = 0;
		for (Text value : values) {
			String d = value.toString();
			cache.add(d);
			sum += Integer.parseInt(d.split(";")[1]);
		}

		for (String value : cache) {
			context.write(new Text(value.split(";")[0] + ";" + key.toString()), new Text(value.split(";")[1] + ";" + sum));
		}
	}
}