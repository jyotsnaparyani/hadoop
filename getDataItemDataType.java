package edu.okstate.cs.EHL.EnhancedMetaDataGenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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

public class getDataItemDataType 
{
	public static class Map extends Mapper<LongWritable,Text,IntWritable, Text> 
	{
		protected void map(LongWritable key, Text value,Mapper.Context context) throws IOException, InterruptedException 
	    {
			String[] values=value.toString().split(",");
			HashMap<Integer,String> map=new HashMap<Integer,String>();
			for(int i=0;i<values.length;i++)
			{
				//System.out.println(values[i]);
				System.out.println(i+1+","+getDatatype(values[i]));
				context.write(new IntWritable(i+1),new Text(getDatatype(values[i])));
			}
	    }
		private String getDatatype(String value)
		{
			if(value.matches("^[0-9]*[.]?[0-9]*[fF]?$")&&!value.equalsIgnoreCase("f"))
			{
				//System.out.println("in digits");
				try{
					if(Integer.parseInt(value)%1==0)
						return "Integer";
				}
				catch(NumberFormatException e)
				{
				if(value.contains("f"))
					return "Float";
				if(value.isEmpty())
					return "Null";
				else
					return "Double";
				
				}
			}
			if(value.equalsIgnoreCase("true")||value.equalsIgnoreCase("false"))
				return "Boolean";
		    return "String";
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
		    	String finalDataType=maxOccDataType(list);
		    	context.write(key, new Text(finalDataType));
		    }
		    public String maxOccDataType(ArrayList dataType)
		    {
		    	HashMap<String,Integer> mapCount=new HashMap<String,Integer>();
		    	int count=0;
		    	String tempKey;
		    	String popularDataType = null;
		    	for(int i=0;i<dataType.size();i++)
		    	{
		    		tempKey=(String)dataType.get(i);
		    		for(int j=0;j<dataType.size();j++)
		    		{
		    			if(tempKey.equalsIgnoreCase((String)dataType.get(j)))
		    				count++;
		    		}
		    		if(!mapCount.containsKey(tempKey))
		    		mapCount.put(tempKey,count);
		    		count=0;
		    	}
		    	int maxValue=Collections.max(mapCount.values());
		    	for(HashMap.Entry<String, Integer> entry:mapCount.entrySet())
		    	{
		    		//System.out.println(entry.getKey()+","+entry.getValue());
		    		if(entry.getValue()==maxValue)
		    			//System.out.println("DataType is :"+entry.getKey());
		    			popularDataType=entry.getKey();
		    	}
		    	return popularDataType;
		    }
	}
	public static void main(String[] args) throws Exception
	{
	    Configuration conf = new Configuration();
	    Job job = new Job(conf);
	    job.setJarByClass(getDataItemDataType.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);

	    job.setMapperClass(getDataItemDataType.Map.class);
	    job.setReducerClass(getDataItemDataType.Reduce.class);

	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    job.waitForCompletion(true);
	  }
}
