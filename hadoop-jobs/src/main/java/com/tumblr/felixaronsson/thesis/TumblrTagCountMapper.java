package com.tumblr.felixaronsson.thesis;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.tumblr.felixaronsson.thesis.UniqueTags.ParseFailures;

/*
 * Input key: 		LongWritable - line number
 * Input value: 	Text - line of file
 * 
 * Output key: 		Text - "Blog id"
 * Output value: 	Text - "Tag name;Tag count"
 */
public class TumblrTagCountMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private List<String> stopWords = Arrays.asList(
		      "a", "an", "and", "are", "as", "at", "be", "but", "by",
		      "for", "if", "im", "in", "into", "is", "it", "its", "me", "my",
		      "no", "not", "of", "on", "or", "queue", "so", "such",
		      "that", "the", "their", "then", "there", "these",
		      "they", "this", "to", "was", "will", "with", "you");
	private boolean use_stopwords;
	
	public void map(LongWritable line, Text data, Context context) throws IOException, InterruptedException {
		String[] strings = data.toString().split("\\t");
		
		if (!strings[3].equals("tag")) {
			context.getCounter(ParseFailures.NOT_A_TAG).increment(1);
			return;
		}
		if (strings[1].length() <= 1) {
			context.getCounter(ParseFailures.EMPTY_TAG).increment(1);
			return;
		}
		if (use_stopwords && stopWords.contains(strings[1])) {
			context.getCounter(ParseFailures.STOPWORD).increment(1);
			return;
		}
		
		context.write(new Text(strings[0]), new Text(strings[1] + ";" + strings[2]));
	}
	
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		use_stopwords = conf.getBoolean("stopwords", false);
	}
}