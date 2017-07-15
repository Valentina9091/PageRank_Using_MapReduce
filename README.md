# PageRank_Using_MapReduce

This code computes the PageRanks of an input set of hyperlinked Wikipedia documents using Hadoop MapReduce.

For the PageRank algorithm itself, refer to Section 2.1.1 of http://infolab.stanford.edu/~backrub/google.html

The inputs to the program are pages from the Simple English Wikipedia.

Each page of Wikipedia is represented in XML as follows:
<title> Page_Name </title>
(other fields we do not care about)
<revision optionalAttr="val">
<text optionalAttr2="val2"> (Page body goes here)
</text>
</revision>

It consists of 2 java files
Java file
	- PageRankAlgorithm.java
	- InvertedIndex.java

Note:
Instructions to create input directory:
-	Create folder in hdfs
	hadoop fs -mkdir /user/cloudera/input

-	Copy all the input file from to hdfs input directory
	hadoop fs -put <source> <destination>
	hadoop fs -put /local/input/file/path /user/cloudera/input

Steps to execute PageRankAlgorithm :

	1.	Create directory build
		mkdir built
		
	2. 	Compile the file java file as 
		javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* PageRankAlgorithm.java -d build -Xlint
		
	3.	Create the jar file as 
		jar -cvf pageRank.jar -C build/ .
		
	4.	Delete the output directory if present and Execute the hadoop code jar as
		Input: <inputFolder> <OutputFolder> 
		hadoop jar pageRank.jar PageRankAlgorithm /user/cloudera/input /user/cloudera/output 
		
	5.	Output file can be found at /user/cloudera/output_sortedOuput (NOTE : "_sortOutput" is concatenate string to the output folder) 
		hadoop fs -ls /user/cloudera/output_sortedOuput
		-	The output file can be viewed using following command:
			Example: hadoop fs -cat /user/cloudera/output_sortedOuput/part-r-00000 
			
		-	To get the file on local machine
			hadoop fs -get /user/cloudera/output_sortedOuput <current directory>
			hadoop fs -get /user/cloudera/output_sortedOuput .

			
Assumptions:
1.	If there is a nested link for example
	[[link [[linknested1] [[linknested2] xyz]]
	outer link is ignored and nested links are considered.
	According to above example linknested1 and linknested2 are considered.
	
2. 	Self loop to the title page is not ignored

3. 	Title with no <Text> tag are considered.

4.  Output does not consider noisy pages.

5. 	Output is sorted in descending order of title and page rank

6. 	PageRankAlgorithm consists of fixed number of iterations that is 10.
	

# Inverted Index
For generating inverted index document Id that is first id tag and text enclosed within text 
tag is extracted. 
Output will be in form of word <tab> list of document ids separated by comma(,).

Assumptions:
1. 	Consider only valid english alphabets. Ignoring digits and special characters
2. 	Considering id from xml file as document ID. That is considering only the first Id after title tag.
3.	All the words are converted to lowercase.
4.	Considering only text which is enclosed within text tag.


Steps to execute PageRankAlgorithm :

	1.	Create directory build
		mkdir built
		
	2. 	Compile the file java file as 
		javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* InvertedIndex.java -d build -Xlint
		
	3.	Create the jar file as 
		jar -cvf invertedIndex.jar -C build/ .
		
	4.	Delete the output directory if present and Execute the hadoop code jar as
		Input: <inputFolder> <OutputFolder> 
		hadoop jar invertedIndex.jar InvertedIndex /user/cloudera/input /user/cloudera/output 
		
	5.	Output file can be found at /user/cloudera/output 
		hadoop fs -ls /user/cloudera/output
		-	The output file can be viewed using following command:
			Example: hadoop fs -cat /user/cloudera/output/part-r-00000 
			
		-	To get the file on local machine
			hadoop fs -get /user/cloudera/output <current directory>
			hadoop fs -get /user/cloudera/output .
