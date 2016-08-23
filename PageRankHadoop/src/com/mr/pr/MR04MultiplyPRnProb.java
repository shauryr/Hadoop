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

public class MR04MultiplyPRnProb {

	public static class EdgelistProbablityPRMapper extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {
			// TODO Auto-generated method stub
			StringTokenizer strTok = new StringTokenizer(value.toString());

			if (strTok.countTokens() != 4)
				return;

			String sVid = strTok.nextToken();
			String oVid = strTok.nextToken();
			String prob = strTok.nextToken();
			String pr = strTok.nextToken();
			output.collect(new Text(oVid), new Text(sVid + " " + prob + " "
					+ pr));
		}
	}

	public static class PageRankProbablityReducer extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			Double result = 0.0;
			while (values.hasNext()) {
				String t = values.next().toString();
				String splitArray[] = t.split(" ");
				result = new Double(splitArray[1]) * new Double(splitArray[2]);

				output.collect(new Text(splitArray[0]), new Text(key.toString()
						+ " " + result.toString()));

			}

		}
	}

}
