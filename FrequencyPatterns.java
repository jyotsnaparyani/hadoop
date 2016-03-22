package edu.okstate.cs.EHL.EnhancedMetaDataGenerator;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class FrequencyPatterns {
	
	public static class FPatternsMapper extends Mapper <Object, Text,  Text, IntWritable> {
		private final static IntWritable one =new IntWritable(1);
		
		public void map(Object key, Text value, Context context) throws IOException,InterruptedException {
			Text pattern = new Text();
			Text newPattern=new Text();
			String textValue =value.toString();
			StringTokenizer tokenizer=new  StringTokenizer(textValue);    //input file is tokenized into words
			while(tokenizer.hasMoreTokens())
			{
				pattern.set(tokenizer.nextToken());
				String token = pattern.toString();
				if(token.matches(".*[^A-Za-z0-9']"))					//regular expression to find the special character in data.
				{
					token = token.substring(token.length()-1);
					newPattern.set(token);
					context.write(newPattern, one);						//key value pair is formed with key as special character and value as 1.
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
				context.write(key, new IntWritable(max));
			}
		      
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf=new Configuration();
		conf.set("mapred.textoutputformat.separator", "+");			//default separator changed to '+'
		Job job=Job.getInstance(conf, "BigDataTestFile");
		job.setJarByClass(FrequencyPatterns.class);
		job.setMapperClass(FPatternsMapper.class);
		job.setCombinerClass(FPatternsReducer.class);
		job.setReducerClass(FPatternsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0: 1);
	}

}
