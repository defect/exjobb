package com.tumblr.felixaronsson.thesis;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.tumblr.felixaronsson.thesis.UniqueTags.ParseFailures;

/*
 * Input key: 		LongWritable - line number
 * Input value: 	Text - line of file
 * 
 * Output key: 		Text - "Tag"
 * Output value: 	IntWritable - 1
 */
public class TumblrUniqueTagsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private List<String> stopWords = Arrays.asList(
		      "a", "an", "and", "are", "as", "at", "be", "but", "by",
		      "for", "if", "in", "into", "is", "it", "me", "my",
		      "no", "not", "of", "on", "or", "such",
		      "that", "the", "their", "then", "there", "these",
		      "they", "this", "to", "was", "will", "with");
	private boolean use_stopwords;
	
	public void map(LongWritable line, Text data, Context context) throws IOException, InterruptedException {
		String[] fields = data.toString().split("\\t");
		if (!fields[3].equals("tag")) {
			context.getCounter(ParseFailures.NOT_A_TAG).increment(1);
			return;
		}
		if (fields[1].length() <= 1) {
			context.getCounter(ParseFailures.EMPTY_TAG).increment(1);
			return;
		}
		if (use_stopwords && stopWords.contains(fields[1])) {
			context.getCounter(ParseFailures.STOPWORD).increment(1);
			return;
		}

		String tagname = fields[1];
		int tagcount = Integer.parseInt(fields[2]);
		context.write(new Text(tagname), new IntWritable(tagcount));
	}
	
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		use_stopwords = conf.getBoolean("stopwords", false);
	}
}