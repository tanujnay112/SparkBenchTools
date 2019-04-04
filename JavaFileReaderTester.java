import com.google.common.base.Charsets;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Random;

/**
 *  * Created by Tanuj on 3/26/19.
 *   */
public class JavaFileReaderTester {
  public static void main(String[] args)
      throws IOException, InterruptedException {
    FileInputStream is = new FileInputStream(args[0]);
    int toRead = Integer.parseInt(args[2]);
    byte buf[] = new byte[Integer.parseInt(args[1])];
    int readBytes = 0;
    long start = System.currentTimeMillis();
    while(readBytes < toRead){
        int chunk = is.read(buf);
        if(chunk < 0)
                break;
        readBytes += chunk;
    }
    long end = System.currentTimeMillis();
    System.out.printf("Read %d bytes in %d ms\n", readBytes, (end-start));
  }
}

