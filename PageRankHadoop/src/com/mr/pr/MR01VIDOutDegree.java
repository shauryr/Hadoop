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

public class MR01VIDOutDegree {

	public static class MapperEdgelist extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		
	

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {
			// TODO Auto-generated method stub
			
			System.out.println("");
			StringTokenizer strTok = new StringTokenizer(value.toString());
			String subject = strTok.nextToken();
			output.collect(new Text(subject), new Text("1"));
		}
	}

	public static class OutDegreeReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			Double degree = 0.0;

			while (values.hasNext()) {
					String t = values.next().toString();
					degree++;
			}
			
			output.collect( key ,
					new Text(new Double(1.0 / degree).toString()));

		}
	}

}
