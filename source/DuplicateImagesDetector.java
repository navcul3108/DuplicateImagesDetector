package source;

import java.io.IOException;
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import source.Skein512;
import source.DIDFileInputFormat;


public class DuplicateImagesDetector {
	final static Text DEFAULT_TEXT_ARRAY[] = new Text[0];
    public static class DuplicateImagesDetectorMapper extends Mapper < Text, BytesWritable, BytesWritable, Text> {
		/*private static Text filenameKey = null;
        @Override
		protected void setup(Context context) throws IOException, InterruptedException {
			InputSplit split = context.getInputSplit();
			Path path = ((FileSplit) split).getPath();
			filenameKey = new Text(path.getParent().getName()+"/"+path.getName());
		}*/
		@Override
        public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException{			
			byte[] imageBytes = value.getBytes();
			byte[] digest = new byte[64];
			Skein512.hash(imageBytes, digest);
			context.write(new BytesWritable(digest), key);
        }
    }

	/*public static class DuplicateImagesDetectorCombiner extends Reducer < Text, BytesWritable, Text, BytesWritable > {
        @Override
        public void reduce(Text key, Iterable < BytesWritable > values, Context context) throws IOException, InterruptedException {
			
            context.write(key, new BytesWritable(bytes));
        }
    }*/

    public static class DuplicateImagesDetectorReducer extends Reducer <BytesWritable, Text, Text, Text> {
        @Override
        public void reduce(BytesWritable key, Iterable < Text> values, Context context) throws IOException, InterruptedException {
			ArrayList<String> fileNames = new ArrayList<>();
            for (Text val: values) {
				fileNames.add(val.toString());
			}			
			if(fileNames.size()>1){
				String keyStr = key.toString().replace(" ", "");
				context.write(new Text(keyStr), new Text(String.join(",", fileNames)));							
			}
        }
    }
    
	
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Image duplication detector");
        job.setJarByClass(DuplicateImagesDetector.class);
        job.setMapperClass(DuplicateImagesDetectorMapper.class);
        //job.setCombinerClass(DuplicateImagesDetectorCombiner.class);
        job.setReducerClass(DuplicateImagesDetectorReducer.class);
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(Text.class);
		job.setInputFormatClass(DIDFileInputFormat.class);
        DIDFileInputFormat.addInputPath(job, new Path(args[0]));
		Path outputDir = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputDir);

		FileSystem hdfs = FileSystem.get(conf);
		if(hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}