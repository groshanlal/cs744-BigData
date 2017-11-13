package storm.starter.cs744Assignment2.Question2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;

import twitter4j.HashtagEntity;
import twitter4j.Status;

@SuppressWarnings("serial")
public class ProcessorBolt extends BaseRichBolt {       
    long _tweetCounter;
    List<String> _hashtags;
    int _friendsCount;
    int _emitCounter;
    HashSet<String> _bufferedTweets;    
    OutputCollector _collector;    
    final static int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 30;    
    
    public ProcessorBolt(List<String> hashtags) {
	_emitCounter = 0;
	_tweetCounter = 0;
	this._hashtags = new ArrayList<String>();
	this._friendsCount = 0;
	this._bufferedTweets = new HashSet<String>();		
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
      this._collector = collector;      
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void execute(Tuple tuple) {
	if (TupleUtils.isTick(tuple)) {	      
	      emitTweets();	      
	} else if (tuple.getSourceComponent().equals("hashtagSpout")) {	    
	    List<String> randomHashtags = (List<String>) tuple.getValue(0);	    
	    _hashtags.clear();
	    _hashtags.addAll(randomHashtags);
	    Collections.sort(_hashtags);
	} else if (tuple.getSourceComponent().equals("friendsCountSpout")) {	    
	    Integer friendCount = (Integer) tuple.getValue(0);	    
	    _friendsCount = friendCount;
	} else {	    
    	    Status tweet = (Status) tuple.getValueByField("tweet");
    	    for (HashtagEntity hashtagEntity : tweet.getHashtagEntities()) {
    		// Check if any of the hashtags value match the current set of hashtags
    		if (Collections.binarySearch(_hashtags, hashtagEntity.getText().toLowerCase()) >= 0
    			&& tweet.getUser().getFriendsCount() < _friendsCount) {
    		    String tweetText = tweet.getText();    		    
    		    _bufferedTweets.add(tweetText);
    		    ++_tweetCounter;
    		}
    	    }
	}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
	ofd.declare(new Fields("emitCounter", "tweet"));
    }
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
      Map<String, Object> conf = new HashMap<String, Object>();
      conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
      return conf;
    }         
    
    private void emitTweets() {	
	for (String tweet : _bufferedTweets) {
	    _collector.emit(new Values(_emitCounter, tweet));
	}		
	++_emitCounter;
	_bufferedTweets.clear();	
    }
}