package edu.okstate.cs.EHL.EnhancedMetaDataGenerator;
//package org.apache.hadoop.fs.shell;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FrequentPatterns 
{
	public static String pattern=null;
	public static int count=0;
	public static class FPatternsMapper extends Mapper <Object, Text,  Text, IntWritable> {
		private final IntWritable one =new IntWritable(1);
		
		public void map(Object key, Text value, Context context) throws IOException,InterruptedException {
			Text pattern = new Text();
			Text newPattern=new Text();
			String textValue =value.toString();
			String[] c=textValue.split("");
			for(int i=0;i<c.length;i++)
			{
				//System.out.println(c[i]);
				if(c[i].matches(".*[^A-Za-z0-9' ]"))					//regular expression to find the special character in data.
				{	
					if(c[i].equals("\t"))
						c[i]="\\t";
					context.write(new Text(c[i]),one);
				}
			}
		}
	}
	
	public static class FPatternsReducer extends Reducer < Text, IntWritable, Text, IntWritable> {
		int max=0;
		
		public void reduce( Text key,Iterable<IntWritable> values, Context context) throws IOException,InterruptedException {
			int patternCount;
			patternCount =0;
			
			for(IntWritable value : values)						//number of times the special character is found in data.
			{
				patternCount += value.get();
			}
			
			if(max<patternCount)								//to find the special character which has occured maximum times.
			{
				max = patternCount;
				pattern=key.toString();
				count=max;
				context.write(key, new IntWritable(max));
			}
		      
		}
	}
	public String getFrequentPatterns(String input) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf=new Configuration();
		conf.set("mapred.textoutputformat.separator", "+");			//default separator changed to '+'
		Job job = new Job(conf);
		job.setJarByClass(FrequentPatterns.class);
		job.setMapperClass(FrequentPatterns.FPatternsMapper.class);
		job.setCombinerClass(FrequentPatterns.FPatternsReducer.class);
		job.setReducerClass(FrequentPatterns.FPatternsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//EnhancedMetaDataGenerator emg=new EnhancedMetaDataGenerator();
		String output=EnhancedMetaDataGenerator.output;
		System.out.println("input path:"+input+" Output:"+output);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output+"/Func1"));
		job.waitForCompletion(true);
		return pattern;
	}
}
