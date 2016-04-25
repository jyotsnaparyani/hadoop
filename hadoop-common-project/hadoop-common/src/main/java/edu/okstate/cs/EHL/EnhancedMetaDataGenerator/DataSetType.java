package edu.okstate.cs.EHL.EnhancedMetaDataGenerator;
//package org.apache.hadoop.fs.shell;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.jdom2.Document;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.json.simple.parser.JSONParser;

public class DataSetType 
{
	static long numberOfRecords=0l;
    static String st="";
	static String filePath="";
	static String s="";
	static String data="";
   
	public static class MetaDataMapper extends Mapper < LongWritable, Text, Text, Text> 
	{
		int max=0;
		//Text maxWord = new Text();
		public void map( LongWritable key,Text values, Context context) throws IOException,InterruptedException 
		{
			
			
			
            String splitting[]=values.toString().split("\\+");
            System.out.println("Length of split:"+splitting.length);
            if(splitting.length>=2)
            {
            	System.out.println("key:"+splitting[0].trim()+" value:"+splitting[1].trim());
			context.write(new Text(splitting[0].trim()), new Text(splitting[1].trim()));
            }
			
		      
		}
	}
	
	public static class MetaDataReducer extends Reducer < Text, Text, Text, Text> 
	{
		int max=0;
		//Text maxWord = new Text();
		public void reduce( Text key,Iterable<Text> values, Context context) throws IOException,InterruptedException 
		{
			
			for(Text value:values)
			{
			System.out.println("Occurances:"+value.toString());
				if(numberOfRecords==Long.parseLong(value.toString()))
				{
					data="S";
					context.write(new Text("S"),new Text(""));
				}
				else 
				{
					System.out.println("in else");
					String type=fileType();
					System.out.println("type:"+type);
					if(type.equals("SS"))
					{
						data="SS";
						context.write(new Text("SS"),new Text(""));
					}
					else
					{
						data="U";
						context.write(new Text("U"),new Text(""));
					}
				}	
			}
		}
	}

    
     static String fileType()
	    {
	    	String type=" ";
			//char ch='';
	    	boolean flag=false;
	    	try
	    	{
	    	//System.out.println(s);
	    	InputStream is=new ByteArrayInputStream(s.getBytes("UTF-8"));
    		SAXBuilder sax=new SAXBuilder();
    		Document doc=sax.build(is);
    		type="SS";
			flag=true;
	    	}
	    	catch(JDOMException e)
	    	{
	    		//type=" ";
	    	} 
	    	catch (IOException e) 
	    	{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	try
	    	{
	    		new JSONParser().parse(s);
	    		type="SS";
	    		flag=true;
	    	}
	    	catch(Exception e)
			{
				//type=" ";
	    		//e.printStackTrace();
			}
	    	System.out.println("flag is:"+flag);
	    	System.out.println("type is:"+type);
	    	return type;
	    }
     public void readData() throws IOException
     {
    	 System.out.println("in readData");
    	 Path pt=new Path(filePath);
         FileSystem fs = FileSystem.get(new Configuration());
         BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
         String line="";
         while ((line=br.readLine())!= null)
			{
           //System.out.println(line);
			  st=line+"\n";
			  st+=st;	
			  if(line!=null)
			  s=s+line;
			  numberOfRecords++;
         }      
     }
	public String getDataSetType(String dataset,String pattern) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException
	{
		EnhancedMetaDataGenerator emg=new EnhancedMetaDataGenerator();
		String output=emg.output;
		Configuration conf=new Configuration();
		Job job=new Job(conf); //change the filename
		job.setJarByClass(DataSetType.class);
		job.setMapperClass(DataSetType.MetaDataMapper.class);
		//job.setCombinerClass(MetaDataGenerator.class);
		System.out.println("in main");
		filePath=dataset;
		readData();
		job.setReducerClass(DataSetType.MetaDataReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(pattern+"/Func1/part-r-00000"));
		FileOutputFormat.setOutputPath(job, new Path(output+"/Func2"));
		job.waitForCompletion(true);
		return data;
	}
	
}
