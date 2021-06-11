package source;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;

public class CompleteFileRecordReader extends RecordReader<Text, BytesWritable> {
	private FileSplit fileSplit;
	private Configuration conf;
	private Text key = new Text();
	private BytesWritable value = new BytesWritable();
	private boolean processed = false;

    public static byte[] toByteArray(BufferedImage bi, String format)
        throws IOException {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ImageIO.write(bi, format, baos);
        byte[] bytes = baos.toByteArray();
        return bytes;
    }

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.fileSplit = (FileSplit) split;
		this.conf = context.getConfiguration();
	}
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!processed) {
			//byte[] contents = new byte[(int) fileSplit.getLength()];z
			Path file = fileSplit.getPath();
			FileSystem fs = file.getFileSystem(conf);
			FSDataInputStream in = null;
			key.set(file.getParent().getName()+"/"+file.getName());
			String format = file.getName().split("\\.")[1];
			try {
				in = fs.open(file);
				//IOUtils.readFully(in, contents, 0, contents.length);
				BufferedImage img = ImageIO.read(in);
				byte[] contents = toByteArray(img, format);
				value.set(contents, 0, contents.length);
			} finally {
				IOUtils.closeStream(in);
			}
			processed = true;
			return true;
		}
		return false;
	}
	
	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}
	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}
	@Override
	public float getProgress() throws IOException {
		return processed ? 1.0f : 0.0f;
	}
	@Override
	public void close() throws IOException {
	// do nothing
	}
}