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

public class MR03JoinPageRankEdgeListProbablity {

	public static class MapperEdgelistProbablity extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {
			// TODO Auto-generated method stub
			StringTokenizer strTok = new StringTokenizer(value.toString());
			String sVid = strTok.nextToken();
			String oVid = strTok.nextToken();
			String prob = strTok.nextToken();
			output.collect(new Text(oVid + "Z"), new Text(sVid + " " + prob));
		}
	}

	public static class PagerankMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {
			// TODO Auto-generated method stub
			StringTokenizer strTok = new StringTokenizer(value.toString());
			String vertex = strTok.nextToken();
			String pr = strTok.nextToken();
			output.collect(new Text(vertex + "A"), new Text(pr));
		}
	}

	public static class PageRankProbablityReducer extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String pr = null;
			String mappedKey = key.toString();
			mappedKey = mappedKey.substring(0, mappedKey.length() - 1);
			int count = 0;
			while (values.hasNext()) {
				String t = values.next().toString();
				String spliarray[] = t.split(" ");
				if (count == 0) {
					count++;
					pr = t;
					continue;
				}

				if (pr == null) {
					throw new IOException("Values are not sorted");
				}
				output.collect(new Text(spliarray[0]), new Text(mappedKey + " "
						+ spliarray[1] + " " + pr));
			}

		}
	}

}
