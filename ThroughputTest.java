import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY;
import static org.apache.hadoop.fs.s3a.Constants.SECRET_KEY;

public class ThroughputTest{
  public static void main(String[] args) throws IOException,
      URISyntaxException {
    S3AFileSystem fs = new S3AFileSystem();
    Configuration conf = new Configuration();
    conf.set(ACCESS_KEY, System.getenv("AWS_ACCESS_KEY_ID"));
    conf.set(SECRET_KEY, System.getenv("AWS_SECRET_ACCESS_KEY"));
    String urlString1 = String.format("s3://tanujspark/%s", args[0]);
    String urlString = "s3://tanujspark/";
    fs.initialize(new URI(urlString), conf);
    FSDataInputStream is = fs.open(new Path(urlString1), Integer.parseInt(args[1]));
    int numBytes = Integer.parseInt(args[2]);
    byte[] buf = new byte[numBytes];
    long start = System.currentTimeMillis();
    long readBytes = 0;
    while(readBytes < numBytes) {
    	int mm = is.getWrappedStream().read(buf);
	if(mm < 0){
	  break;
	}
	readBytes += mm;
    }
    long end = System.currentTimeMillis();
    System.out.printf("TOOK %d ms to read %d bytes\n", (end-start), readBytes);
  }
}
