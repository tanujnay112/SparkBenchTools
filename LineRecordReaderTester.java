import com.google.common.base.Charsets;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;

import java.io.IOException;
import java.util.Random;

/**
 *  * Created by Tanuj on 3/26/19.
 *   */
public class LineRecordReaderTester {
  public static void main(String[] args)
      throws IOException, InterruptedException {
    JobConf job = new JobConf();
    job.setInt("io.file.buffer.size", 64*1024);
    job.set("fs.s3a.access.key", System.getenv("AWS_ACCESS_KEY_ID"));
    job.set("fs.s3a.secret.key", System.getenv("AWS_SECRET_ACCESS_KEY"));
    FileSplit split = new FileSplit(new Path(args[0]),
        Long.parseLong(args[1]), Long.parseLong(args[2]), job);
    String delimiter = "\n";
    LineRecordReader reader = new LineRecordReader(job, split, delimiter.getBytes(Charsets.UTF_8));
    LongWritable key = new LongWritable();
    Text val = new Text();
    Random rand = new Random();
    while(reader.next(key, val)) {
      if(rand.nextInt(10) >= 2)
        continue;
      Thread.sleep(0);
    }
  }
}

