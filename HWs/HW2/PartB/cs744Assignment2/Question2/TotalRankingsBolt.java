package storm.starter.cs744Assignment2.Question2;

import java.util.ArrayList;
import java.util.Collections;
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
public class TotalRankingsBolt extends BaseRichBolt {           
    List<Word> _topWords;
    int _emitCounter;
    OutputCollector _collector;
    
    public TotalRankingsBolt() {
	_emitCounter = 0;
	_topWords = new ArrayList<Word>();
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
      this._collector = collector;      
    }
        
    @SuppressWarnings("unchecked")
    @Override
    public void execute(Tuple tuple) {
	Integer emitCounter = (Integer) tuple.getValueByField("emitCounter");
	List<Word> topWords = (List<Word>) tuple.getValueByField("topWords");	
	if (emitCounter != _emitCounter) {	    
	    emitTopWords(emitCounter);
	}
	_topWords.addAll(topWords);
	
    }

    private void emitTopWords(Integer newEmitCounter) {
	Collections.sort(_topWords);
	_collector.emit("topWordsStream", new Values(_emitCounter, "", 0)); // Dummy header word, removed later in RecordFormat.
	for (int i = 0; i < _topWords.size(); ++i) {
	    _collector.emit("topWordsStream", new Values(_emitCounter, _topWords.get(i).getWord(), _topWords.get(i).getCount()));	    
	}
	_collector.emit("emitCounterStream", new Values(_emitCounter));
	
	_emitCounter = newEmitCounter;
	_topWords.clear();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
	ofd.declareStream("emitCounterStream", new Fields("emitCounter"));
	ofd.declareStream("topWordsStream", new Fields("emitCounter", "topWord", "count"));		
    }    
    
}