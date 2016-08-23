package com.mr.pr;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MR02JoinEdgeListOutDegree {

	public static class MapperEdgelist extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {
			// TODO Auto-generated method stub
			StringTokenizer strTok = new StringTokenizer(value.toString());
			String subject = strTok.nextToken();
			String object = strTok.nextToken();
			output.collect(new Text(subject + "Z"), new Text(object));
		}
	}

	public static class MapperVertexProbablity extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {
			// TODO Auto-generated method stub
			StringTokenizer strTok = new StringTokenizer(value.toString());
			String vertex = strTok.nextToken();
			String probablity = strTok.nextToken();
			output.collect(new Text(vertex + "A"), new Text(probablity));
		}
	}

	public static class OutDegreeReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String probablity = null;
			String mappedKey = key.toString();
			mappedKey = mappedKey.substring(0, mappedKey.length() - 1);
			int count = 0;
			while (values.hasNext()) {
				String t = values.next().toString();
				if (count == 0) {
					count++;
					probablity = t;
					continue ;
				}
				
				if (probablity == null) {
					throw new IOException("Values are not sorted");
				}
				output.collect(new Text(mappedKey), new Text(t + " "+probablity));
			}

		}
	}

}
