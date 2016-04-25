package edu.okstate.cs.EHL.EnhancedMetaDataGenerator;
//package org.apache.hadoop.fs.shell;
import java.io.IOException;
import java.net.URISyntaxException;

public class EnhancedMetaDataGenerator 
{
	public  String output=null;
	public void run(String inputPath)
	{
		//String inputPath=args[0];
		output="/EMGoutput";
		FrequentPatterns fr=new FrequentPatterns();
		DataSetType func2=new DataSetType();
		DataItems func3=new DataItems();
		DataItems_SS func4=new DataItems_SS();
		DataItemDataType func5=new DataItemDataType();
		DataItemUnique func6=new DataItemUnique();
		DataItemUnique_SS func7=new DataItemUnique_SS();
		Concatenator concat=new Concatenator();
		try 
		{
			//fr.getFrequentPatterns(inputPath);
			String freqPattern=fr.getFrequentPatterns(inputPath);
			String data=func2.getDataSetType(inputPath, output);
			if(data.equalsIgnoreCase("SS"))
			{
				func4.getDataItems_SS(inputPath);
				func7.isDataItemUnique_SS(inputPath);
				concat.concatenate(data,inputPath);
			}
			//System.out.println("Pattern is: "+freqPattern);
			if(data.equalsIgnoreCase("S")||data.equalsIgnoreCase("U"))
			{
				func3.getDataItems(inputPath, freqPattern);
				func5.getDataItemDataType(inputPath, freqPattern);
				func6.isDataItemUnique(inputPath, freqPattern);
				concat.concatenate(data,inputPath);
			}
			
		} 
		catch (ClassNotFoundException | IOException | InterruptedException | URISyntaxException e) 
		{
			e.printStackTrace();
		}
	}
}
