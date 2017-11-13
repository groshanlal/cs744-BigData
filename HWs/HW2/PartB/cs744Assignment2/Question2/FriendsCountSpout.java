package storm.starter.cs744Assignment2.Question2;

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
public class FriendsCountSpout extends BaseRichSpout {
    public static Logger LOG = LoggerFactory.getLogger(TestWordSpout.class);
    boolean _isDistributed;
    SpoutOutputCollector _collector;
    List<Integer> _friendsCount;
    final static long EMIT_TIME_INTERVAL_IN_MILLISECONDS = 30000;    

    public FriendsCountSpout() {
        this(false);
    }

    public FriendsCountSpout(boolean isDistributed) {
        _isDistributed = isDistributed;
    }
    
    public FriendsCountSpout(List<Integer> friendsCount) {
	_friendsCount = friendsCount;
    }
        
    @SuppressWarnings("rawtypes")
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }
    
    public void close() {
        
    }
        
    public void nextTuple() {	
        final Random rand = new Random();        
        int randomFriendCount = _friendsCount.get(rand.nextInt(_friendsCount.size()));
        _collector.emit(new Values(randomFriendCount));
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