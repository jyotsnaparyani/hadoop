package edu.okstate.cs.EHL.EnhancedMetaDataGenerator;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
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



public class getDataItems_SS 
{
	/*
	 * XmlJsonInputFormat is custom input format class to pass data to the mapper.
	 */
	public static class XmlJsonInputFormat extends TextInputFormat
	{
  

	    public RecordReader<LongWritable, Text> createRecordReader(
	            InputSplit split, TaskAttemptContext context) {
	        return new XmlJsonRecordReader();
	    }

	    /**
	     * XmlJsonRecordReader class to read through a given xml document to output
	     * xml blocks as records
	     *
	     */
	    
	    
	//custom
	 public static class XmlJsonRecordReader extends
	    RecordReader<LongWritable, Text> {
	        private byte[] startTag;
	        private byte[] endTag;
	        private long start;
	        private long end;
	        private FSDataInputStream fsin;
	        private DataOutputBuffer buffer = new DataOutputBuffer();

	        private LongWritable key = new LongWritable();
	        private Text value = new Text();
	        InputStream is;
			SAXBuilder sax;
			Document doc;
			private static StringBuilder b=new StringBuilder();
	        @Override
	        public void initialize(InputSplit split, TaskAttemptContext context)
	        throws IOException, InterruptedException {
	            Configuration conf = context.getConfiguration();
	            FileSplit fileSplit = (FileSplit) split;
	            // open the file and seek to the start of the split
	            start = fileSplit.getStart();
	            end = start + fileSplit.getLength();
	            Path file = fileSplit.getPath();
	            FileSystem fs = file.getFileSystem(conf);
	            fsin = fs.open(fileSplit.getPath());
	            fsin.seek(start);
	        }
	        @Override
	        public boolean nextKeyValue() throws IOException,
	        InterruptedException {
	            if (fsin.getPos() < end) 
	            {
	            	System.out.println("POS:"+fsin.getPos());
	            	int i = 0;
	                while (true) 
	                {
	                    int b = fsin.read();
	                    // end of file:
	                        if (b == -1)
	                            break;
	                    // save to buffer:
	                            buffer.write(b);
	                }
	                key.set(fsin.getPos());
	                value.set(buffer.getData(), 0,
	                        buffer.getLength());
	                return true;
	            }
	            return false;
	        }
	        @Override
	        public LongWritable getCurrentKey() throws IOException,
	        InterruptedException {
	            return key;
	        }

	        @Override
	        public Text getCurrentValue() throws IOException,
	        InterruptedException {
	            return value;
	        }
	        @Override
	        public void close() throws IOException {
	            fsin.close();
	        }
	        @Override
	        public float getProgress() throws IOException {
	            return (fsin.getPos() - start) / (float) (end - start);
	        }

	        
	}

	 }
	/*
	 * Map class parses the xml/json data and decides the data type of each data item i.e., key in case of json and tag in case of xml
	 */
	public static class Map extends Mapper<LongWritable,Text,Text, Text> 
	{
		public static StringBuilder b=new StringBuilder();
		public  ArrayList<String> list=new ArrayList<String>();
		public String arrayKey;
	    @Override
	    protected void map(LongWritable key, Text value,Mapper.Context context) throws IOException, InterruptedException 
	    {
	        String document = value.toString();
	        String file=fileType(document);
	        /*
	         * if file is of xml type, then it extracts the key and content 
	         */
	        if(file.equalsIgnoreCase("xml"))
	        {
	        try 
	        {
	        	InputStream is=new ByteArrayInputStream(document.getBytes("UTF-8"));
	    		SAXBuilder sax=new SAXBuilder();
	    		Document doc=sax.build(is);
	    		Element classEl=doc.getRootElement();
	    		b.append(classEl.getName()+"\tvoid;");
	    		List<Element> stList=classEl.getChildren();
	    		printNode(stList, classEl);
	    		StringTokenizer st1=new StringTokenizer(b.toString(),";");
	    		while(st1.hasMoreTokens())
	    		{
	    			String[] tokens=st1.nextToken().split("\t");
	    			if(tokens.length>=2)
	    			{
	    				if(tokens[1].equalsIgnoreCase("void"))
	    					tokens[1]="NULL";
	    			System.out.println("key:"+tokens[0]+" value:"+tokens[1]);
	    			String dataType=getDatatype(tokens[1]);
	    			context.write(new Text(tokens[0]),new Text(dataType));
	    			}
	    		}
	        }
	        catch(Exception e){
	            throw new IOException(e);

	        }
	        }
	        else if(file.equalsIgnoreCase("json"))
	        {
	        	String jsonData=value.toString();
	    		if(jsonData.startsWith("["))
	    			parseJsonArray( jsonData);
	    		else if(jsonData.startsWith("{"))
	    			parseJsonObject(jsonData);
	    		 Iterator<String> iterator = list.iterator();
	    		 System.out.println("in list");
	    		 System.out.println("size:"+list.size());
	    	     while(iterator.hasNext())
	    	     {
	    	         String values=(String)iterator.next();
	    	         String[] tokens=values.split("\t");
	    	         if(tokens.length>=2)
	    	         {
	    	        	// System.out.println("key:"+tokens[0]+" value:"+tokens[1]);
	    	        	 String dataType=getDatatype(tokens[1]);
	    	        	 context.write(new Text(tokens[0]),new Text(dataType));
	    	         }
	    	     }
	        }
	        else
	        	System.out.println("Not an XML/JSON File");
	        
	    }
	    /*
	     * Gives the type of file(XML or JSON)
	     */
	    private String fileType(String data)
	    {
	    	String type="";
	    	try
	    	{
        	InputStream is=new ByteArrayInputStream(data.getBytes("UTF-8"));
    		SAXBuilder sax=new SAXBuilder();
    		Document doc=sax.build(is);
    		type="xml";
	    	}
	    	catch(JDOMException e)
	    	{
	    		System.out.println("Not an xml File");
	    	} 
	    	catch (IOException e) 
	    	{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	try
	    	{
	    		new JSONParser().parse(data);
	    		type="json";
	    	}
	    	catch(Exception e)
			{
				System.out.println("not a json file");
			}
	    	return type;
	    }
	    private void printNode(List<Element> el,Element classEl)
		{
			if(el!=null && el.size()>0)
			{
				for(int i=0;i<el.size();i++)
				{
					Element node=el.get(i);
					System.out.println(node.getName());
					//System.out.println(node.getName()+":"+getValue(node, classEl));
					List<Element> childList= node.getChildren();
					if(childList.size()==0)
					{
						System.out.println("in printnode");
						if(node.getValue().isEmpty())
						{
							System.out.println("in spl loop");
							b.append(node.getName()+"\tvoid;");
						}				
						else
						b.append(node.getName()+"\t"+node.getValue()+";");
						System.out.println(node.getName()+"\t"+node.getValue()+";");
					}
					else
						b.append(node.getName()+"\tvoid;");
					printNode(node.getChildren(),classEl);
				}
			}
		}
	    private String getDatatype(String value)
		{
			if(value.matches("^[0-9]*[.]?[0-9]*[fF]?$"))
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
				else
					return "Double";
				
				}
			}
			if(value.equalsIgnoreCase("true")||value.equalsIgnoreCase("false"))
				return "Boolean";
	        return "String";
		}
	    public void parseJsonArray(String data)
		{
			try 
			{
				JSONParser parser=new JSONParser();
				JSONArray json=(JSONArray)parser.parse(data);
				for(int i=0;i<json.size();i++)
				{
					//System.out.println("key:"+json.get(i).toString()+" value:"+json.get(i).toString());
					//System.out.println("in array"+json.get(i).toString());
					
					if(json.get(i).toString().startsWith("{"))
						parseJsonObject(json.get(i).toString());
					else if(json.get(i).toString().startsWith("["))
					{
						if(json.get(i).toString().contains(":")||json.get(i).toString().contains("{"))
						parseJsonArray(json.get(i).toString());
					}
					else
					{
						//System.out.println("key:"+arrayKey+" value:"+json.get(i).toString());
						list.add(arrayKey+"\t"+json.get(i).toString());
					}
				}
			} 
			catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		public void parseJsonObject(String data)
		{
			try 
			{
				JSONParser parser=new JSONParser();
				JSONObject json=(JSONObject)parser.parse(data);
					for(Object key:json.keySet())
					{
						if(json.get(key).toString().startsWith("["))
						{
							if(json.get(key).toString().contains(":")||json.get(key).toString().contains("{"))
							{
							//System.out.println("key:"+key.toString()+" value:VOID");
							list.add(key.toString()+"\tVOID");
							}
							arrayKey=key.toString();
							parseJsonArray( json.get(key).toString());
						}	
						else if(json.get(key).toString().startsWith("{"))
						{
							//System.out.println("key:"+key.toString()+" value:VOID");
							list.add(key.toString()+"\tVOID");
							parseJsonObject(json.get(key).toString());
						}
						else
						{
							//System.out.println("key:"+key.toString()+" value:"+json.get(key).toString());
							list.add(key.toString()+"\t"+json.get(key).toString());
						}
					}
					//if()
					
			} 
			catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	/*
	 * Gives max occurance of a datatype
	 */
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

	   // private Text outputKey = new Text();
	    public void reduce(Text key, Iterable<Text> values,Context context)
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
	    job.setJarByClass(getDataItems_SS.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

	    job.setMapperClass(getDataItems_SS.Map.class);
	    job.setReducerClass(getDataItems_SS.Reduce.class);

	    job.setInputFormatClass(XmlJsonInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    job.waitForCompletion(true);
	  }
}
