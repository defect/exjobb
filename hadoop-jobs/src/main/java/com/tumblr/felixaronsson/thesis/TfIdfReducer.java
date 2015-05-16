package com.tumblr.felixaronsson.thesis;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class TfIdfReducer extends Reducer<Text, Text, Text, Text> {
	
	private int num_docs;
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		long sum = 0;
		ArrayList<String> cache = new ArrayList<String>();
		for (Text value : values) {
			sum++; 
			cache.add(value.toString());
		}

		for(String item : cache) {
			float n_i = Integer.parseInt(item.split(";")[1]);
			float n_k = Integer.parseInt(item.split(";")[2]);
			float tf = (n_i/n_k);
			double idf = Math.log((num_docs/(float)sum));
			double tfidf = tf*idf;
			//				        <Tag name>; <Object name>                 <TF> ;    <IDF>  ;    <TFIDF>  ;    <raw TF>
			context.write(new Text(key + ";" + item.split(";")[0]), new Text(tf + ";" + idf + ";" + tfidf + ";" + n_i));
		}
	}
	
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		num_docs = conf.getInt("num_docs", 20000);
	}
}