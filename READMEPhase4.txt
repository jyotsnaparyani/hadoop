1. A java file EnhancedMetaDataGenerator which is integrated using the 8 sub-functions should be executed using the EMG.jar file using the following command.
	hadoop jar EMG.jar inputDatasetPath OutputDatasetPath
2. Changed Hadoop Source Code in the below path in github account for implementing the function whenver the datset is imported into hdfs
	hadoop/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/shell/CommandWithDestination.java
3. The changes are made especially in copyFileToTarget  function by calling run function from the EnhancedMetaDataGenerator.java file.
4. Changed Hadoop Source Code in the below path in github account for implementing the function whenver the datset is deleted from hdfs
	hadoop/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/shell/Delete.java
5. The changes are made especially in moveToTrash function by calling deleteMeta function from DeleteMetaData.java file.
