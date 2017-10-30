package storm.starter.cs744Assignment2.Question1;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;


public class Main{
	private static final String HDFS_ABSOLUTE_URI_PATTERN = "hdfs://[\\d\\.:]+";
	private static final String TOPOLOGY_NAME = "CS838_Assignment2_PartC_Question1";

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        String consumerKey = "lwWcYNl1viTgtn2rSiVLCsKlF"; 
        String consumerSecret = "v445enVVKpJjvKZyOxw3ZotfRu0cusgkuWlH1yNCMtxIKzYw44"; 
        String accessToken = "4195089552-B9vFOBbl6ZahxrLj446VV4GPPnVM85e0ftcqU8f"; 
        String accessTokenSecret = "IW6Mk8OQOCsWHljwgHXEdLxsSpVi99p11NzStWhF1Tcrj";
        
        String mode = args[0];
        String outputFilepath = args[1];
        
        String[] arguments = args.clone();
        String[] keyWords = Arrays.copyOfRange(arguments, 2, arguments.length);
        
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("twitterSpout", new TwitterSpout(consumerKey, consumerSecret,
                                accessToken, accessTokenSecret, keyWords, TOPOLOGY_NAME, mode), 5)
        	.setNumTasks(4);
        
        if (outputFilepath.startsWith("hdfs")) {                       
            Pattern pat = Pattern.compile(HDFS_ABSOLUTE_URI_PATTERN);
            Matcher m = pat.matcher(outputFilepath);
            String fsUrl = null;
            String fsPath = null;
            if (m.find()) {
        	fsUrl = m.group(0);
        	String uri[] = outputFilepath.split(HDFS_ABSOLUTE_URI_PATTERN);
        	if (uri.length == 2) {
        	    fsPath = uri[1];
        	} else {
        	    System.err.println("Expected absolute HDFS uri, e.g. hdfs://128.104.223.115:8020/user/ubuntu/output/");
        	    System.exit(1);
        	}
            } else {
        	System.err.println("Expected absolute HDFS uri, e.g. hdfs://128.104.223.115:8020/user/ubuntu/output/");
        	System.exit(1);
            }
            
            RecordFormat format = new DelimitedRecordFormat();
            SyncPolicy syncPolicy = new CountSyncPolicy(1000);
            FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.GB);
            FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(fsPath);
                                    
            HdfsBolt hdfsBolt = new HdfsBolt();
            hdfsBolt.withFsUrl(fsUrl)
            	    .withFileNameFormat(fileNameFormat)
            	    .withRecordFormat(format)
            	    .withSyncPolicy(syncPolicy)
            	    .withRotationPolicy(rotationPolicy);
            builder.setBolt("hdfsBolt", hdfsBolt, 2).shuffleGrouping("twitterSpout").setNumTasks(5);
        } else {
            builder.setBolt("localFileWriterBolt", new WriteLocalBolt(outputFilepath))
                   .shuffleGrouping("twitterSpout");
        }
        
                
        if (mode.equals("cluster")) {        
            Config conf = new Config();
            conf.setNumWorkers(20);
            conf.setMaxSpoutPending(5000);
            StormSubmitter.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
            displayInitMessage(outputFilepath);
        } else {
            Config conf = new Config();                   
            LocalCluster cluster = new LocalCluster();        
            cluster.submitTopology("test", conf, builder.createTopology());
            displayInitMessage(outputFilepath);
            Utils.sleep(120000);
            cluster.shutdown();            
        }
    }
	
    private static void displayInitMessage(String outputFilepath) {		
	System.out.println("Output of the application available at path: " + outputFilepath);
    }
}