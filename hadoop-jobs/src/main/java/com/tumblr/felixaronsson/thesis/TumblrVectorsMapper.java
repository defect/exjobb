package com.tumblr.felixaronsson.thesis;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.VectorWritable;

import com.tumblr.felixaronsson.thesis.LastVectors.Skipped;


public class TumblrVectorsMapper extends Mapper<Text, Text, Text, VectorWritable> {
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
		
		type = conf.get("type");
		
		FileSystem fs = FileSystem.get(URI.create("/user/felixaronsson/"), conf);
		Path path = new Path(conf.get("dictionary"));
		
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
		
		Text key = new Text();
		IntWritable value = new IntWritable();
		
		while (reader.next(key, value)) {
			dictionary.put(key.toString(), value.get());
		}
		reader.close();
	}
}