package edu.okstate.cs.EHL.EnhancedMetaDataGenerator;
//package org.apache.hadoop.fs.shell;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class Concatenator 
{
	public void concatenate(String type,String input) throws IOException
	{
		String output=EnhancedMetaDataGenerator.output;
		System.out.println("in concat");
		System.out.println("type is :"+type);
		String inputPath=input.substring(input.indexOf("/"),input.lastIndexOf("/")+1);
		int dot=input.lastIndexOf(".");
		String dataset="";
		if(dot==-1)
			dataset=input.substring(input.lastIndexOf("/")+1);
		else
			dataset=input.substring(input.lastIndexOf("/")+1,input.lastIndexOf("."));
		String dest=inputPath+dataset+".metadata";
		System.out.println("input:"+input);
		System.out.println("dataset:"+dataset);
		System.out.println("dest:"+dest);
		System.out.println("Output:"+output);
		File file=new File(dataset+".metadata");
		if (!file.exists()) {
			file.createNewFile();
		}

		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		bw.write("DATA-ITEM,DATA-TYPE,isUNIQUE\n");
		if(type.equalsIgnoreCase("U")||type.equalsIgnoreCase("S"))
		{
			String func3=output+"/Func3/part-r-00000";
			System.out.println(func3);
			String func5=output+"/Func5/part-r-00000";
			String func6=output+"/Func6/part-r-00000";
			String headers=getData(func3);
			List<String> dataItems=readData(headers, 1);
			String types=getData(func5);
			List<String> dataTypes=readData(types, 2);
			String unique=getData(func6);
			List<String> isUnique=readData(unique, 2);
		Iterator<String> itrData=dataItems.iterator();
		Iterator<String> itrType=dataTypes.iterator();
		Iterator<String> itrUnique=isUnique.iterator();
		System.out.println("DATA-ITEM,DATA-TYPE,isUNIQUE");
		while(itrData.hasNext()&&itrType.hasNext()&&itrUnique.hasNext())
		{
			String d1=itrData.next();
			String d2=itrType.next();
			String d3=itrUnique.next();
			System.out.println(d1+","+d2+","+d3);
			bw.write(d1+","+d2+","+d3+"\n");
			
		}
		}
		if(type.equalsIgnoreCase("SS"))
		{
			String func4=output+"/Func4/part-r-00000";
			System.out.println(func4);
			String func7=output+"/Func7/part-r-00000";
			String headers=getData(func4);
			List<String> dataItems=readData(headers, 1);
			//String types=getData("/EMGoutput/Func5/part-r-00000");
			List<String> dataTypes=readData(headers, 2);
			String unique=getData(func7);
			List<String> isUnique=readData(unique, 2);
			Iterator<String> itrData=dataItems.iterator();
			Iterator<String> itrType=dataTypes.iterator();
			Iterator<String> itrUnique=isUnique.iterator();
			System.out.println("DATA-ITEM,DATA-TYPE,isUNIQUE");
			while(itrData.hasNext()&&itrType.hasNext()&&itrUnique.hasNext())
			{
				String d1=itrData.next();
				String d2=itrType.next();
				String d3=itrUnique.next();
				System.out.println(d1+","+d2+","+d3);
				bw.write(d1+","+d2+","+d3+"\n");
			}
		}
		InputStream is=new BufferedInputStream(new FileInputStream("dataset.metadata"));
		Configuration conf = new Configuration();
		System.out.println("Connecting to -- "+conf.get("fs.defaultFS"));
		 
		//Destination file in HDFS
		FileSystem fs = FileSystem.get(URI.create(dest), conf);
		OutputStream out = fs.create(new Path(dest));
		 
		//Copy file from local to HDFS
		IOUtils.copyBytes(is, out, 4096, true);
		 
		System.out.println(dest + " copied to HDFS");
	}
	public String getData(String path) throws IllegalArgumentException, IOException
	{
		FileSystem fs = FileSystem.get(new Configuration());
	    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
	    String line="";
		String data="";
		//numberOfLines=0;
	    while ((line=br.readLine()) != null)
		{
	      //System.out.println(line);
	    	data=data+line+"\n";
	    	//numberOfLines++;
	    }
	    return data;
	}
	public List<String> readData(String data,int pos)
	{
		List<String> list=new ArrayList<String>();
		//String[] cell=new String[numberOfLines];
		String[] lines = data.split(System.getProperty("line.separator"));
		for(int i=0;i<lines.length;i++)
		{
			String[] cells=lines[i].split("\t");
			list.add(cells[pos-1]);
			//cell[i]=cells[pos];
		}
		return list;
	}
}
