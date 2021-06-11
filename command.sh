hadoop com.sun.tools.javac.Main ./Source/*.java
jar cf ./Source/ImageDuplicationDetector.jar ./Source/*.class
hadoop jar ./Source/ImageDuplicationDetector.jar Source.ImageDuplicationDetector /user/student116/Assignment/input/image_test /user/student116/Assignment/output