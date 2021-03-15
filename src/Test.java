import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class Test {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get( new URI( "hdfs://localhost:8020" ),conf,"root" );
//        fs.copyFromLocalFile( new Path( "/Users/yinjin/Desktop/hadoop" ),new Path( "/" ) );
        fs.delete( new Path( "/hadoop" ), true  );
        fs.close();
    }
}
