package com.tumblr.felixaronsson.thesis;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.GenericsUtil;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.VectorWritable;

import com.tumblr.felixaronsson.thesis.LastVectors.Skipped;


public class LastVectorsMapper extends Mapper<Text, Text, Text, VectorWritable> {
	private VectorWritable writer;
	private Map<String,Integer> dictionary = new HashMap<String,Integer>();
	private String type;

	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {		
		writer = new VectorWritable();

		String tag = key.toString().split(";")[0];
		String artist = key.toString().split(";")[1];

		double weight = 0;
		if (type.equals("tfidf")) {
			weight = Double.parseDouble(value.toString().split(";")[2]);
		}
		else if (type.equals("tf")) {
			weight = Double.parseDouble(value.toString().split(";")[3]);
		}
		else {
			throw new IllegalArgumentException();
		}

		NamedVector vector = new NamedVector(new SequentialAccessSparseVector(dictionary.size()), artist);
		try {
			vector.set(dictionary.get(tag), weight);
			writer.set(vector);
			context.write(new Text(artist), writer);
		}
		catch (Exception e) {
			context.getCounter(Skipped.TAG).increment(1);
		}
	}
	
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		DefaultStringifier<Map<String,Integer>> mapStringifier = new DefaultStringifier<Map<String,Integer>>(
				conf, GenericsUtil.getClass(dictionary));

		dictionary = mapStringifier.fromString(conf.get("tag_dict"));
		type = conf.get("type");
	}
}