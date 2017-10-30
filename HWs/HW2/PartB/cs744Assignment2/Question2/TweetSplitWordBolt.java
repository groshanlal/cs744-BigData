package storm.starter.cs744Assignment2.Question2;

import java.util.HashSet;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

@SuppressWarnings("serial")
public class TweetSplitWordBolt extends BaseRichBolt {           
    HashSet<String> _stopwords;
    OutputCollector _collector;
    
    public TweetSplitWordBolt(HashSet<String> stopwords) {	
	this._stopwords = stopwords;
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
      this._collector = collector;      
    }
        
    @Override
    public void execute(Tuple tuple) {
	Integer emitCounter = (Integer) tuple.getValueByField("emitCounter");
	String tweet = (String) tuple.getValueByField("tweet");
	String words[] = tweet.split("\\s+");
	for (String word : words) {	    
	    String emitWord = word.trim().toLowerCase();
	    // Remove '#' from hashtags, if at all.
	    if (emitWord.charAt(0) == '#') {
		emitWord = emitWord.substring(1);
	    }
	    if (!isStopWord(emitWord)) {
		_collector.emit(new Values(emitCounter, emitWord));		
	    }
	}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
	ofd.declare(new Fields("emitCounter", "word"));
    }    
    
    private boolean isStopWord(String word) {
	if (word.length() == 0) return true;
	if (_stopwords.contains(word)) return true;
	for (int i = 0; i < word.length(); ++i) {
	    if (!Character.isAlphabetic(word.charAt(i))) {
		return true;
	    }
	}
	return false;
    }
}