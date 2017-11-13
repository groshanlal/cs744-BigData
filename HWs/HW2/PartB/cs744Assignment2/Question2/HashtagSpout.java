package storm.starter.cs744Assignment2.Question2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class HashtagSpout extends BaseRichSpout {
    public static Logger LOG = LoggerFactory.getLogger(TestWordSpout.class);
    boolean _isDistributed;
    SpoutOutputCollector _collector;
    List<String> _hashtags;
    final static long EMIT_TIME_INTERVAL_IN_MILLISECONDS = 30000;
    int _emit_hashtag_count = 5;

    public HashtagSpout() {
        this(false);
    }

    public HashtagSpout(boolean isDistributed) {
        _isDistributed = isDistributed;
    }
    
    public HashtagSpout(List<String> hashtags) {
	_hashtags = hashtags;
	_emit_hashtag_count = (hashtags.size() / 2);
    }
        
    @SuppressWarnings("rawtypes")
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }
    
    public void close() {
        
    }
        
    public void nextTuple() {        
        List<String> randomHashtags = new ArrayList<String>();
        final Random rand = new Random();       
        for (int i = 0; i < _emit_hashtag_count; ++i) {
            String randomHashtag = _hashtags.get(rand.nextInt(_hashtags.size())).toLowerCase();
            if (randomHashtags.contains(randomHashtag)) {
        	--i; continue; // Repeat again, if duplicate.        	
            }
            randomHashtags.add(randomHashtag);
        }
        _collector.emit(new Values(randomHashtags));
        Utils.sleep(EMIT_TIME_INTERVAL_IN_MILLISECONDS);
    }
    
    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {
        
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        if(!_isDistributed) {
            Map<String, Object> ret = new HashMap<String, Object>();
            ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
            return ret;
        } else {
            return null;
        }
    }    
}