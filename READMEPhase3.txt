5. getDataItemDataType.java file contains the implementation of obtaining frequently occurred datatype in a particular column from any kind of file.
   Functionalities of this function include
   a) Taking any kind of file as input and frequently occurring patterns in the input file to the getDataItemDataType.java
   b) Mapper provides the output as column number occurred in the file as KEYS with the data type of the value in that particular column as VALUES.
   c) Reducer provides output as column number occurred in the file as KEY with the datatype which occurred most number of times in the corresponding column             number is retrieved as VALUE.
   To execute the getDataItemDataType.jar, any kind of file  and frequently occurring patterns in that file should be given as input and the following command is to be    used.
	hadoop jar getDataItemDataType.jar edu.okstate.cs.EHL.EnhancedMetaDataGenerator.FrequencyPatterns inputFilePath FrequentPattern OutputDirectory
   To view the output file
	hadoop dfs -cat OutputFilePath
6. isDataItemUnique.java file contains implementation of identification of duplicate values in each and every column from any kind of file which is structured and    unstructured.
   Functionalities of this function include
   a) Taking any kind of file as input and frequently occurring patterns in the input file to the isDataItemUnique.java
   b) Mapper provides the output as column number occurred in the file as KEYS with the value in that column as VALUES.
   c) Reducer provides output as column number occurred in the file as KEYS with resulting either YES if all the column values in that particular column number are all       unique or NO if all the column values in that particular column number are not unique(duplicated).
   To execute the isDataItemUnique.jar, any kind of file  and frequently occurring patterns in that file should be given as input and the following command is to be       used.
	hadoop jar isDataItemUnique.jar edu.okstate.cs.EHL.EnhancedMetaDataGenerator.FrequencyPatterns inputFilePath FrequentPattern OutputDirectory
   To view the output file
	hadoop dfs -cat OutputFilePath
7. isDataItemUnique_SS.java file contains implementation of identification of duplicate values in each and every column from XML file which is semi-structured.
   Dependencies of this function include adding external jars namely
   1)json-simple-1.1.jar
   2)jdom-2.0.6.jar
   Functionalities of this function include 
   a) Taking XML file as input to the isDataItemUnique_SS.java
   b) Mapper provides the output as tags in the XML file as KEYS with the value in the corresponding tags as VALUES.
   c) Reducer provides output as tags in the XML file as KEYS with resulting either YES if all the column values in that particular tag are all unique or NO if all the       column values in that particular tag are not unique(duplicated).
   To execute the isDataItemUnique_SS.jar, XML file should be given as input and the following command is to be used.
	hadoop jar isDataItemUnique_SS.jar edu.okstate.cs.EHL.EnhancedMetaDataGenerator.FrequencyPatterns inputFilePath OutputDirectory
   To view the output file
	hadoop dfs -cat OutputFilePath