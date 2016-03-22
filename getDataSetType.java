package edu.okstate.cs.EHL.EnhancedMetaDataGenerator;


import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;




public class getDataSetType
{

    static long numberOfRecords=0l;
    static String st="";
	static String filePath="";
	static String s="";

   
	public static class MetaDataMapper extends Mapper < LongWritable, Text, Text, Text> 
	{
		int max=0;
		//Text maxWord = new Text();
		public void map( LongWritable key,Text values, Context context) throws IOException,InterruptedException 
		{
			
			
			Path pt=new Path(filePath);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line="";
			
            line=br.readLine();
            while (line != null)
			{
              //System.out.println(line);
              line=br.readLine();
			  st=line+"\n";
			  st+=st;
			  s=s+line;
			  numberOfRecords++;
            }      
            String splitting[]=values.toString().split("\\s+");
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
					context.write(value,new Text("S"));
				}
				else 
				{
					System.out.println("in else");
					String type=fileType();
					if(type==" ")
					{
						context.write(value,new Text("U"));
					}
					else
					{
						context.write(value,new Text("SS"));
					}
				}	
			}
		}
	}

    
     static String fileType()
	    {
	    	String type="";
			//char ch='';
	    	try
	    	{
        	InputStream is=new ByteArrayInputStream(s.getBytes("UTF-8"));
    		SAXBuilder sax=new SAXBuilder();
    		Document doc=sax.build(is);
    		type="SS";
			
	    	}
	    	catch(JDOMException e)
	    	{
	    		type=" ";
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
	    	}
	    	catch(Exception e)
			{
				type=" ";
			}
	    	return type;
	    }
		
		
		public static void main(String[] args) throws Exception
	{
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf); //change the filename
		job.setJarByClass(getDataSetType.class);
		job.setMapperClass(getDataSetType.MetaDataMapper.class);
		//job.setCombinerClass(MetaDataGenerator.class);
		//System.out.println("in main");
		filePath=args[2];
		job.setReducerClass(getDataSetType.MetaDataReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0: 1);
	}

		
		
		
}		





		
		
