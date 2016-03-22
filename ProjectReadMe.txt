1. FrequencyPatterns.java file contains the implementation of obtaining frequent patterns from any kind of file.
   Functionalities of this function include
   a) Taking any kind of file as input to the FrequencyPatterns.java
   b) Mapper provides the output as special characters occurred in the file as KEYS with the number of times a particular characters occurred as VALUES.
   c) Reducer provides output as frequently occurred special character in the file as KEY with the number of times it occurred as VALUE.
   To execute the FrequencyPatterns.jar, any kind of file should be given as input and the following command is to be used.
	hadoop jar FrequencyPatterns.jar edu.okstate.cs.EHL.EnhancedMetaDataGenerator.FrequencyPatterns inputFilePath OutputDirectory
   To view the output file
	hadoop dfs -cat OutputFilePath

2. getDataSetType.java file contains the implementation of obtaining the type of the data involved in any kind of file.
   Functionalities of this function include
   a) Taking any kind of file as input to the getDataSetType.java
   b) Mapper provides the output as frequently occurred special character in the file as KEY and number of times it occurred as VALUE
   c) Reducer provides the output as a string describing the type of the dataset (i.e., 'S'- when dataset line count equals to VALUE from mapper; 
      'SS'- when dataset line count not equal to VALUE from mapper and the dataset does not give any exception when it is parsed using XML/JSON parser;
      'U'- when dataset line count not equal to VALUE from mapper and the dataset encounters exception when it is parsed using XML/JSON parser.) as KEY and 
      value obtained from mapper as VALUE.
   To execute the getDataSetType.java, a XML/JSON file should be given as input and the following command is to be used.
	hadoop jar getDataSetType.jar edu.okstate.cs.EHL.EnhancedMetaDataGenerator.getDataSetType inputFilePath OutputDirectory
   To view the output file
	hadoop dfs -cat OutputFilePath

3. getDataItems.java file contains the implementation of finding out the maximum occurred header name for the input CSV file
   Functionalities of this function include
   a) Taking a CSV file as input to the getDataItems.java
   b) Mapper provides the output as position of the data in the dataset (i.e., column number assuming dataset as a table in the database) as KEY and appropriate
      name for the key using neural network tool like WEKA as VALUE
   c) Reducer provides output as all the unique positions of the dataset (i.e., all the column numbers) as KEYS and maximum occurred appropriate name corresponding
      to key as VALUE.
   To execute the getDataItems.java, a CSV file, trained ARFF file and an ARFF file corresponding to the CSV file should be given as input and the following
   command is to be used.
	hadoop jar getDataItems.jar edu.okstate.cs.EHL.EnhancedMetaDataGenerator.getDataItems inputCSVfilePath OutputDirectory TrainedARFFfile CSVtoRFFfile
   To view the output file
	hadoop dfs -cat OutputFilePath

4. getDataItems_SS.java file contains the implementation of obtaining the maximum occurred data types of the unique keys in the XML/JSON file.
   Functionalities of this function include
   a) Taking a XML/Json file as input to the getDataItems_SS.java
   b) Mapper provides the output as tags occurred in the file as KEYS and type of the tag as VALUE.
   c) Reducer provides output as unique tags as KEYS and maximum occurred data type for a particular key as VALUE.
   To execute the getDataItems_SS.java, a XML/JSON file should be given as input and the following command is to be used.
	hadoop jar getDataItems_SS.jar edu.okstate.cs.EHL.EnhancedMetaDataGenerator.getDataItems_SS inputFilePath OutputDirectory
   To view the output file
	hadoop dfs -cat OutputFilePath


