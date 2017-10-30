package storm.starter.cs744Assignment2.Question2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

@SuppressWarnings("serial")
public class TweetAccumulatorBolt extends BaseRichBolt {  
    OutputCollector _collector;    
    Map<Integer, List<String>> _bufferedTweets;
    
    public TweetAccumulatorBolt() {
	_bufferedTweets = new HashMap<Integer, List<String>>();	
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
      this._collector = collector;      
    }
        
    @Override
    public void execute(Tuple tuple) {
	if (tuple.getSourceComponent().equals("totalRankingsBolt")) {
	    // Time to emit the tweets.
	    Integer emitCounter = (Integer) tuple.getValueByField("emitCounter");
	    emitTweets(emitCounter);
	} else {
	    // Buffer the tweet.
	    Integer emitCounter = (Integer) tuple.getValueByField("emitCounter");
	    String tweet = (String) tuple.getValueByField("tweet");
	    bufferTweet(emitCounter, tweet);
	}					
    }    

    private void bufferTweet(Integer emitCounter, String tweet) {
	if (_bufferedTweets.containsKey(emitCounter)) {
	    _bufferedTweets.get(emitCounter).add(tweet);
	} else {
	    List<String> tweets = new ArrayList<String>();
	    tweets.add(tweet);
	    _bufferedTweets.put(emitCounter, tweets);
	}
	
    }

    private void emitTweets(Integer emitCounter) {
	System.out.println("Emitting tweets and top words for emit counter : " + emitCounter);
	_collector.emit(new Values(emitCounter, ""));  // Dummy header tweet, removed later in RecordFormat.
	List<String> tweets = _bufferedTweets.get(emitCounter);
	for (String tweet : tweets) {
	    _collector.emit(new Values(emitCounter, tweet));
	}
	_bufferedTweets.remove(emitCounter);	
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
	ofd.declare(new Fields("emitCounter", "tweet"));
    }    
}