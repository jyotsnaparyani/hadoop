package edu.okstate.cs.EHL.EnhancedMetaDataGenerator;
//package org.apache.hadoop.fs.shell;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DataItemUnique 
{
	public static String freqPattern=null;
	public static class Map extends Mapper<LongWritable,Text,IntWritable, Text> 
	{
		protected void map(LongWritable key, Text value,Mapper.Context context) throws IOException, InterruptedException 
	    {
			String[] values=value.toString().split(freqPattern);
			HashMap<Integer,String> map=new HashMap<Integer,String>();
			for(int i=0;i<values.length;i++)
			{
				//System.out.println(values[i]);
				//System.out.println(i+1+","+getDatatype(values[i]));
				context.write(new IntWritable(i+1),new Text(values[i]));
			}
	    }
		
	}
	public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> 
	{

		   // private Text outputKey = new Text();
		    public void reduce(IntWritable key, Iterable<Text> values,Context context)
		    throws IOException, InterruptedException 
		    {
		    	ArrayList list=new ArrayList();
		    	for(Text value:values)
		    	{
		    		list.add(value.toString());
		    	}
		    	String unique=null;
		    	if(isUnique(list))
		    		unique="Yes";
		    	else
		    		unique="No";
		    	context.write(key, new Text(unique));
		    }
		    public boolean isUnique(ArrayList dataType)
		    {
		    	HashMap<String,Integer> mapCount=new HashMap<String,Integer>();
		    	int count=0;
		    	String tempKey;
		    	boolean flag = true;
		    	for(int i=0;i<dataType.size();i++)
		    	{
		    		tempKey=(String)dataType.get(i);
		    		for(int j=0;j<dataType.size();j++)
		    		{
		    			if(i!=j)
		    			{
		    			if(tempKey.equalsIgnoreCase((String)dataType.get(j)))
		    				return false;
		    			}
		    		}
		    	}
		    	return flag;	
		    }
	}
	public void isDataItemUnique(String dataset,String pattern) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException
	{
		 Configuration conf = new Configuration();
		 EnhancedMetaDataGenerator emg=new EnhancedMetaDataGenerator();
			String output=emg.output;
		    Job job = new Job(conf);
		    job.setJarByClass(DataItemUnique.class);
		    job.setOutputKeyClass(IntWritable.class);
		    job.setOutputValueClass(Text.class);
		    freqPattern=pattern;
		    job.setMapperClass(DataItemUnique.Map.class);
		    job.setReducerClass(DataItemUnique.Reduce.class);

		    job.setInputFormatClass(TextInputFormat.class);
		    job.setOutputFormatClass(TextOutputFormat.class);

		    FileInputFormat.addInputPath(job, new Path(dataset));
		    FileOutputFormat.setOutputPath(job, new Path(output+"/Func6"));

		    job.waitForCompletion(true);
	}
}
