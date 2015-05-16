package com.tumblr.felixaronsson.thesis;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.GenericsUtil;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import com.tumblr.felixaronsson.thesis.LastVectors.Skipped;

public class TumblrVectorsReducer extends Reducer<Text, VectorWritable, Text, VectorWritable> {

	private VectorWritable writer = new VectorWritable();
	private double percentage; 

	public void reduce(Text artist, Iterable<VectorWritable> values, Context context) throws IOException, InterruptedException {

		Vector reducedVector = null;
		for (VectorWritable partial: values) {
			if(reducedVector == null) {
				reducedVector = partial.get().like();
			}			
			for (Vector.Element ve : partial.get().nonZeroes()) {
				reducedVector.set(ve.index(), ve.get());
			}
		}

		NamedVector namedVector = new NamedVector(reducedVector, artist.toString());
		writer.set(namedVector);
		
		if(Math.random() < percentage) {
			context.write(new Text(artist), writer);
		}
		else {
			context.getCounter(Skipped.ARTIST).increment(1);
		}
	}
	
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		percentage = conf.getFloat("percentage", 100)/100.0;
	}
}