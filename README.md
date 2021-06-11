# DuplicateImagesDetector
Detect all duplicate images on large dataset using Hadoop and Skein Hash function family.  
**Author**: Giang Van Luc
## Idea
Read content of eachfile, then hash with Skein algorithm. Images whose hash output are the same will be duplicate. Group it with Hadoop MapReduce.  
## Requirements
    1. Hadoop is installed and configured on your computer
    2. 
## Run code
Make sure that you are on the root of this project.
1. Compile **.java** file to **.class** file.    
```
hadoop com.sun.tools.javac.Main ./source/*.java
```
2. Combine all **.class** files to **.jar** file.
```
jar cf ./source/DuplicateImagesDetector.jar ./source/*.class
```
3. Create your input folder in order to contain images.
```
hadoop fs -mkdir /user/\<#your_username\>/DuplicateImagesDetector/input
```
4. Create output folder.
```
hadoop fs -mkdir /user/\<#your_username\>/DuplicateImagesDetector/output
```
5. Put your image folder to input folder.
```
hadoop fs -put /link/to/your/images/folder /user/\<#your_username\>/DuplicateImagesDetector/input
```
6. Run with your images.
```
hadoop jar ./source/DuplicateImagesDetector.jar source.DuplicateImagesDetector /user/student116/DuplicateImagesDetector/input/<#your_input_folder_name> /user/student116/Assignment/output/<#your_input_folder_name>
```
7. Copy output file to local.
```
hadoop fs -get /user/student116/Assignment/output/<#your_input_folder_name>/part-r-00000 ./output/
```
