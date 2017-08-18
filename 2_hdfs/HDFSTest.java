import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFSTest {
    public static void main(String[] args) throws IOException {

    	try {
	    	String hdfsPath = "hdfs://localhost:9000";
	    	Configuration config = new Configuration();
			FileSystem fs = FileSystem.get(config);

	    	config.set("fs.defaultFS", hdfsPath);

	    	// Make a dir in location /test/dir
	    	Path hdfsDirPath = new Path("/test/dir");
	    	Boolean success = false;

	    	if (!fs.exists(hdfsDirPath)) {
	    		fs.mkdirs(hdfsDirPath);
	    	}

	    	// Copy this .java from the local file system to the new created directory in the HDFS
	    	Path localFilePath = new Path("HDFSTest.java");

	    	fs.copyFromLocalFile(localFilePath, hdfsDirPath);

	    	// Create a new file in the HDFS and append some data into it
	    	Path hdfsNewFile = new Path(hdfsDirPath.toString() + "/newFile.txt");

	        FSDataOutputStream newFile = fs.create(hdfsNewFile);

	        newFile.writeChars("This is a test file\n");
	        newFile.writeChars("and we are just writing some bytes into it\n");
	        newFile.close();

	        // Move the new file from HDFS to the local file system
	        fs.copyToLocalFile(hdfsNewFile, new Path("."));

	        // Close fs
        	fs.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
    }
}