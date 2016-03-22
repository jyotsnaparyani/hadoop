package edu.okstate.cs.EHL.EnhancedMetaDataGenerator;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author sri
 */


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import weka.classifiers.Evaluation;
import weka.classifiers.functions.MultilayerPerceptron;
import weka.core.Utils;
import weka.core.converters.ArffSaver;
import weka.core.converters.CSVLoader;



public class getDataItems
{
    static String trainingSet,dataSetArff,outputPath;
	public static class Map extends Mapper<LongWritable, Text,IntWritable,Text>
	{
		protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
		{
                    Configuration conf=context.getConfiguration();
                    ArrayList list=new ArrayList();
			String[] columns=value.toString().split(",");
                        ArrayList<String> headerList=Weka();
			for(int i=0;i<columns.length;i++)
			{
				int pos=i+1;
                                list.add(pos+"\t"+columns[i]);
                                
				//context.write(new IntWritable(pos),new Text(columns[i]));
			}
                        Iterator it=headerList.iterator();
                        int j=0;
                        while(it.hasNext())
                        {
                           if(j==columns.length)
                            j=1;
                           j++;
                           String headers=(String) it.next();
                           context.write(new IntWritable(j),new Text(headers));
                           
                        }
                        
		}
	
        //to convert CSV file to arff
        
        //to predict the datatype of dataitems
        public ArrayList<String> Weka()
        { 
                ArrayList<String> headers=new ArrayList<String>();
                try
                { 
                    FileReader trainreader = new FileReader(trainingSet); 
//FileReader testreader = new FileReader("C:\\Users\\sri\\Documents\\datasets\\test1.arff"); 


                    weka.core.Instances train = new weka.core.Instances(trainreader); 
//Instances test = new Instances(testreader); 
                    train.setClassIndex(train.numAttributes() - 1); 
//test.setClassIndex(test.numAttributes() - 1); 

                    MultilayerPerceptron mlp = new MultilayerPerceptron(); 
                    mlp.setOptions(Utils.splitOptions("-L 0.3 -M 0.2 -N 500 -V 0 -S 0 -E 20 -H 4")); 


                    mlp.buildClassifier(train); 
                    Evaluation eval = new Evaluation(train);
                    eval.evaluateModel(mlp, train);
                    //System.out.println(eval.errorRate()); //Printing Training Mean root squared Error
                    // System.out.println(eval.toSummaryString()); //Summary of Training
                    eval.crossValidateModel(mlp, train, 10, new Random(1));
                        
                    //evaluating with a test set
                    /*Evaluation eval = new Evaluation(train); 
                    eval.evaluateModel(mlp, test); 
                    System.out.println(eval.toSummaryString("\nResults\n======\n", false)); 
                    */
                    FileReader fr=new FileReader(dataSetArff);
                    BufferedReader br= new BufferedReader(fr);
                    weka.core.Instances datapredict = new weka.core.Instances(br);
                    int num=datapredict.numAttributes();
                    datapredict.setClassIndex(num-1);
                    //BufferedWriter writer = new BufferedWriter(
                    //new FileWriter(outputPath));
                    
                    weka.core.Instances predicteddata = new weka.core.Instances(datapredict);
                    //Predict Part
                    for (int i = 0; i < datapredict.numInstances(); i++) 
                    {
                        double clsLabel = mlp.classifyInstance(datapredict.instance(i));
                        predicteddata.instance(i).setClassValue(clsLabel);
                        //System.out.println(predicteddata.instance(i).stringValue(num-1));
                        //writer.write(predicteddata.instance(i).stringValue(num-1));
                        headers.add(predicteddata.instance(i).stringValue(num-1));
                    }
                    //Storing again in a    rff
                        
                    //writer.write(predicteddata.toString());
                        
                    //writer.newLine();
                    //writer.flush();
                    //writer.close();
                    trainreader.close(); 
                    
                        

            }
            catch(Exception ex)
            { 

                ex.printStackTrace(); 
                

            }
                return headers;
//return null;
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

	
public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
{
	Configuration conf=new Configuration();
      trainingSet=args[2];
      dataSetArff=args[3];
      //outputPath=args[1];
	Job job=new Job(conf);
	job.setJarByClass(getDataItems.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass(getDataItems.Map.class);
    job.setReducerClass(getDataItems.Reduce.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
}
}
