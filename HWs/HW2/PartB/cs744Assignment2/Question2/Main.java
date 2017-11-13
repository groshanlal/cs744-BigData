package storm.starter.cs744Assignment2.Question2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class Main {
    
    private static final String STOPWORD_FILEPATH = "/home/ubuntu/software/apache-storm-1.0.2/examples/storm-starter/src/jvm/storm/starter/cs744Assignment2/Question2/stopwords.txt";
    private static final String HDFS_ABSOLUTE_URI_PATTERN = "hdfs://[\\d\\.:]+";
    private static final String TOPOLOGY_NAME = "CS744_Assignment2_Partb_Question2";
    
    public static void main(String[] args) throws IOException, AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        String consumerKey = "lwWcYNl1viTgtn2rSiVLCsKlF"; 
        String consumerSecret = "v445enVVKpJjvKZyOxw3ZotfRu0cusgkuWlH1yNCMtxIKzYw44"; 
        String accessToken = "4195089552-B9vFOBbl6ZahxrLj446VV4GPPnVM85e0ftcqU8f"; 
        String accessTokenSecret = "IW6Mk8OQOCsWHljwgHXEdLxsSpVi99p11NzStWhF1Tcrj";       
        List<String> hashtags = new ArrayList<String>();
        List<Integer> friendsCount = new ArrayList<Integer>();
        String mode = args[0];
        String outputFilepath = args[1];
        for (int i = 2; i < args.length; ++i) {
            try {
        	friendsCount.add(Integer.parseInt(args[i]));
            } catch (NumberFormatException e) {
        	hashtags.add(args[i]);
            }
        }
                
        // Load stopwords into a hashset
        HashSet<String> stopwords = new HashSet<String>();
        String line = null;
        BufferedReader br = new BufferedReader(new FileReader(STOPWORD_FILEPATH));
        while ((line = br.readLine()) != null) {
            stopwords.add(line.trim().toLowerCase());
        }
        br.close();
        

        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("twitterSpout", new TwitterSpout(consumerKey, consumerSecret,
                                accessToken, accessTokenSecret, hashtags), 4)
               .setNumTasks(5);
        builder.setSpout("hashtagSpout", new HashtagSpout(hashtags));
        builder.setSpout("friendsCountSpout", new FriendsCountSpout(friendsCount));
        builder.setBolt("processorBolt", new ProcessorBolt(hashtags), 2)
        	.setNumTasks(5)
                .shuffleGrouping("twitterSpout")
                .allGrouping("hashtagSpout")
                .allGrouping("friendsCountSpout");        
        builder.setBolt("tweetSplitWordBolt", new TweetSplitWordBolt(stopwords), 2)
        	.setNumTasks(5)
        	.shuffleGrouping("processorBolt");        
        builder.setBolt("intermediateRankingBolt", new IntermediateRankingBolt(), 2)
        	.setNumTasks(5)
        	.fieldsGrouping("tweetSplitWordBolt", new Fields("word"));
        builder.setBolt("totalRankingsBolt", new TotalRankingsBolt(), 1)
        	.globalGrouping("intermediateRankingBolt");
		
        builder.setBolt("tweetAccumulatorBolt", new TweetAccumulatorBolt())        	
		.shuffleGrouping("processorBolt")
		.allGrouping("totalRankingsBolt", "emitCounterStream");
        
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
            
            RecordFormat format = new EmitRecordFormat();
            SyncPolicy syncPolicy = new CountSyncPolicy(1000);
            FileRotationPolicy rotationPolicy = new EmitCounterRotationPolicy();
            FileNameFormat fileNameFormat = new OutputFileNameFormat().withPrefix("tweets").withPath(fsPath);                                    
            HdfsBolt hdfsTweetBolt = new HdfsBolt();
            hdfsTweetBolt.withFsUrl(fsUrl)
            	    .withFileNameFormat(fileNameFormat)
            	    .withRecordFormat(format)
            	    .withSyncPolicy(syncPolicy)
            	    .withRotationPolicy(rotationPolicy);
            builder.setBolt("hdfsTweetWriterBolt", hdfsTweetBolt).shuffleGrouping("tweetAccumulatorBolt");
                        
            format = new EmitRecordFormat();
            syncPolicy = new CountSyncPolicy(1000);
            rotationPolicy = new EmitCounterRotationPolicy();
            fileNameFormat = new OutputFileNameFormat().withPrefix("topWords").withPath(fsPath);
            HdfsBolt hdfsTopWordsBolt = new HdfsBolt();
            hdfsTopWordsBolt.withFsUrl(fsUrl)
            	    .withFileNameFormat(fileNameFormat)
            	    .withRecordFormat(format)
            	    .withSyncPolicy(syncPolicy)
            	    .withRotationPolicy(rotationPolicy);
            builder.setBolt("hdfsTopWordWriterBolt", hdfsTopWordsBolt).shuffleGrouping("totalRankingsBolt", "topWordsStream");
        }
                
                
        if (mode.equals("cluster")) {        
            Config conf = new Config();
            conf.setNumWorkers(20);
            conf.setMaxSpoutPending(50000);
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
	System.out.println("Initializing and stabiling the topology for initial set of tweets...");
	System.out.println("\n********************************************************************");
	System.out.println("********************************************************************\n");
	System.out.println("Output of the application available at path: " + outputFilepath);
    }

}