package com.mr.pr;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapred.lib.MultipleInputs;

public class MainScoring {

	public static void main(String[] args) throws IOException {
		MainScoring thisObj = new MainScoring();

		if (args.length < 1) {
			System.err
					.println("Usage: argument list -  <VertexMappingPath> <outputPath> <Input path>");
		}
	//	String pageranks = args[1];
		String outputPath = args[2];
		String inputPath = args[0];

		thisObj.getMR01(inputPath, outputPath + "/01/");

		thisObj.getMR02(inputPath, outputPath + "/01/", outputPath + "/02/");
		Integer j = 0;
		Integer k = 0;
		for (Integer i = 0; i < 5; i ++) {
			System.out.println(i);
			thisObj.getMR03(
					outputPath + "/0" + new Integer(2).toString() + "/",
					outputPath + "/0" + new Integer(j + 3).toString()
							+ "pagerank/", outputPath + "/0"
							+ new Integer(k + 3).toString() + "/");
			k++; 
			thisObj.getMR04(outputPath + "/0" + new Integer(k + 2).toString()
					+ "/", outputPath + "/0" + new Integer(k + 3).toString()
					+ "/");
			k++ ;
			thisObj.getMR05(outputPath + "/0" + new Integer(k + 2).toString()
					+ "/", outputPath + "/0" + new Integer(j + 4).toString()
					+ "pagerank/");
			j++ ;
		}

	}

	private void getMR05(String edgelistProbablityPagerank, String outputPath)
			throws IOException {
		JobConf conf = new JobConf(com.mr.pr.MR05SumPRnProb.class);
		conf.setJobName("DD-MR04-Annotate Subj VID Obj VID");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setReducerClass(com.mr.pr.MR05SumPRnProb.PageRankProbablityReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		MultipleInputs.addInputPath(conf, new Path(edgelistProbablityPagerank),
				TextInputFormat.class,
				com.mr.pr.MR05SumPRnProb.EdgelistProbablityPRMapper.class);

		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		JobClient.runJob(conf);

		System.out.println("Output-Path=" + outputPath);
		System.out
				.println("====================================================================");
	}

	private void getMR04(String edgelistProbablityPagerank, String outputPath)
			throws IOException {
		JobConf conf = new JobConf(com.mr.pr.MR04MultiplyPRnProb.class);
		conf.setJobName("DD-MR04-Annotate Subj VID Obj VID");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setReducerClass(com.mr.pr.MR04MultiplyPRnProb.PageRankProbablityReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		MultipleInputs.addInputPath(conf, new Path(edgelistProbablityPagerank),
				TextInputFormat.class,
				com.mr.pr.MR04MultiplyPRnProb.EdgelistProbablityPRMapper.class);

		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		JobClient.runJob(conf);

		System.out.println("Output-Path=" + outputPath);
		System.out
				.println("====================================================================");
	}

	private void getMR03(String edgelistprobablity, String pagerank,
			String outputPath) throws IOException {
		JobConf conf = new JobConf(
				com.mr.pr.MR03JoinPageRankEdgeListProbablity.class);
		conf.setJobName("DD-MR03-Annotate Subj VID Obj VID");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setReducerClass(com.mr.pr.MR03JoinPageRankEdgeListProbablity.PageRankProbablityReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		MultipleInputs
				.addInputPath(
						conf,
						new Path(edgelistprobablity),
						TextInputFormat.class,
						com.mr.pr.MR03JoinPageRankEdgeListProbablity.MapperEdgelistProbablity.class);
		MultipleInputs
				.addInputPath(
						conf,
						new Path(pagerank),
						TextInputFormat.class,
						com.mr.pr.MR03JoinPageRankEdgeListProbablity.PagerankMapper.class);

		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setOutputValueGroupingComparator(SortReducerByValuesValueGroupingComparator.class);
		conf.setPartitionerClass(SortReducerByValuesPartitioner.class);

		conf.setOutputValueGroupingComparator(SortReducerByValuesValueGroupingComparator.class);
		conf.setPartitionerClass(SortReducerByValuesPartitioner.class);

		JobClient.runJob(conf);

		System.out.println("Output-Path=" + outputPath);
		System.out
				.println("====================================================================");
	}

	private void getMR02(String edgelist, String probablity, String outputPath)
			throws IOException {
		JobConf conf = new JobConf(com.mr.pr.MR02JoinEdgeListOutDegree.class);
		conf.setJobName("DD-MR03-Annotate Subj VID Obj VID");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setReducerClass(com.mr.pr.MR02JoinEdgeListOutDegree.OutDegreeReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		MultipleInputs.addInputPath(conf, new Path(edgelist),
				TextInputFormat.class,
				com.mr.pr.MR02JoinEdgeListOutDegree.MapperEdgelist.class);
		MultipleInputs
				.addInputPath(
						conf,
						new Path(probablity),
						TextInputFormat.class,
						com.mr.pr.MR02JoinEdgeListOutDegree.MapperVertexProbablity.class);

		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setOutputValueGroupingComparator(SortReducerByValuesValueGroupingComparator.class);
		conf.setPartitionerClass(SortReducerByValuesPartitioner.class);

		conf.setOutputValueGroupingComparator(SortReducerByValuesValueGroupingComparator.class);
		conf.setPartitionerClass(SortReducerByValuesPartitioner.class);

		JobClient.runJob(conf);

		System.out.println("Output-Path=" + outputPath);
		System.out
				.println("====================================================================");
	}

	private void getMR01(String inputPath, String outputPath)
			throws IOException {
		JobConf conf = new JobConf(com.mr.pr.MR01VIDOutDegree.class);
		conf.setJobName("DD-MR01-SubjectObjectPairs");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(com.mr.pr.MR01VIDOutDegree.MapperEdgelist.class);
		conf.setReducerClass(com.mr.pr.MR01VIDOutDegree.OutDegreeReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		JobClient.runJob(conf);

		System.out.println("Output-Path=" + outputPath);
		System.out
				.println("====================================================================");
	}

	public static final class SortReducerByValuesValueGroupingComparator
			implements RawComparator<Text> {
		@Override
		public int compare(byte[] text1, int arg1, int arg2, byte[] text2,
				int arg4, int arg5) {
			return new Character((char) text1[0]).compareTo((char) text2[0]);
		}

		@Override
		public int compare(Text key1, Text key2) {
			String stK1 = key1.toString().substring(0,
					key1.toString().length() - 1);
			String stK2 = key2.toString().substring(0,
					key2.toString().length() - 1);
			int val = compare(stK1.getBytes(), 0, stK1.length(),
					stK2.getBytes(), 0, stK2.length());
			val = stK1.compareTo(stK2);
			return val;
		}
	}

	public static final class SortReducerByValuesPartitioner extends
			HashPartitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			return super
					.getPartition(
							new Text(key.toString().substring(0,
									key.toString().length() - 1)), value,
							numPartitions);
		}
	}

}
