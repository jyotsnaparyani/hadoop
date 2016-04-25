//package edu.okstate.cs.EHL.EnhancedMetaDataGenerator;
package org.apache.hadoop.fs.shell;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DeleteMetaData 
{
	public void deleteMeta(String path) throws IOException
	{
		FileSystem hdfs =FileSystem.get(new Configuration());
		Path workingDir=hdfs.getWorkingDirectory();
		Path newFolderPath= new Path(path);
		newFolderPath=Path.mergePaths(workingDir, newFolderPath);

		if(hdfs.exists(newFolderPath))

		{

		hdfs.delete(newFolderPath, true); //Delete existing Directory

		}
	}
}
