This project is for COSC 2637/2633 Big Data Processing Assignment 1.

********************************************

Where is the jar files?

The jar files for Task 1 is in Assignment2Task1/target. The standalone jar is called 'Assignment2Task1-0.0.1-SNAPSHOT-shaded.jar'.
The jar files for Task 2 is in Assignment2Task2/target. The standalone jar is called 'Assignment2Task2-0.0.1-SNAPSHOT-shaded.jar'
The jar files for Task 3 is in Assignment2Task3/target. The standalone jar is called 'Assignment2Task3-0.0.1-SNAPSHOT-shaded.jar'

********************************************

How to use the standalone jar files?

When you are in the hadoop platform, enter the following command for each task:

1. For Task 1
hadoop jar Assignment2Task1-0.0.1-SNAPSHOT-shaded.jar group.s3749857.Assignment2Task1.CooccurrenceFrequency <HDFS_input_folder> <HDFS_output_folder>
2. For Task 2
hadoop jar Assignment2Task2-0.0.1-SNAPSHOT-shaded.jar group.s3749857.Assignment2Task2.RelativeFrequency <HDFS_input_folder> <HDFS_output_folder>
3. For Task 3
hadoop jar Assignment2Task3-0.0.1-SNAPSHOT-shaded.jar group.s3749857.Assignment2Task3.PAM <HDFS_input_file> <HDFS_output_folder> <k value>

Important tips:
1. The input path for Task 3 should represents a file, not a folder.
2. The k value for Task 3 should be integer and in the range of 1 to 10.