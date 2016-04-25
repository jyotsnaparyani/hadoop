package edu.okstate.cs.EHL.EnhancedMetaDataGenerator;
//package org.apache.hadoop.fs.shell;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
public class DataItems 

{

  public void getDataItems(String dataset,String pattern) throws URISyntaxException, InterruptedException 
  {
	String output=EnhancedMetaDataGenerator.output;  
	//String csvFile = "C:\\Users\\sri\\Documents\\datasets\\yob1880.csv";
	BufferedReader br = null;
	String line = "";
	//String cvsSplitBy = ",";

	try {

		Path pt=new Path(dataset);
        FileSystem fs = FileSystem.get(new Configuration());
		br = new BufferedReader(new InputStreamReader(fs.open(pt)));
                line = br.readLine();
                String[] header= line.split("\\r?\\n");
                Configuration configuration = new Configuration();
                String port=getHdfs();
                FileSystem hdfs = FileSystem.get( new URI( port ), configuration );
                Path file = new Path(port+output+"/Func3/part-r-00000");
                if ( hdfs.exists( file )) { hdfs.delete( file, true ); } 
                OutputStream os = hdfs.create( file,
                    new Progressable() {
                        public void progress() {
                          // System.out.println("...bytes written: [ "+bytesWritten+" ]");
                        } });
                BufferedWriter brw = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
               // br.write("Hello World");
                //br.close();
                //hdfs.close();
                String[] header1=header[0].split(pattern);
            	   for(int i=0;i<header1.length;i++)
            	   {                         
            		   System.out.println(i+1+" , "+header1[i]);
            		   brw.write(header1[i]);
            		   brw.write("\n");
            	   }
            	   brw.flush();
            	   brw.close();
            	   hdfs.close();

	} catch (FileNotFoundException e) {
		e.printStackTrace();
	} catch (IOException e) {
		e.printStackTrace();
	} 

	System.out.println("Done");
  }
  public String getHdfs() throws IOException, InterruptedException
  {
	  Process proc = Runtime.getRuntime().exec("hdfs getconf -confKey fs.default.name");
      // Read the output
      BufferedReader reader =  
            new BufferedReader(new InputStreamReader(proc.getInputStream()));
      String line = "";
      String port="";
      while((line = reader.readLine()) != null) 
      {
         // System.out.print(line + "\n");
          if(line.contains("hdfs://"))
          	port=line;
      }
      //System.out.println("port:"+port);
      proc.waitFor();   
      return port;
  }
}
