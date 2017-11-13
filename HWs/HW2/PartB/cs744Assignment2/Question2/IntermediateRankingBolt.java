package storm.starter.cs744Assignment2.Question2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

@SuppressWarnings("serial")
public class IntermediateRankingBolt extends BaseRichBolt {  
    OutputCollector _collector;
    int _emitCounter;
    Map<String, Word> _wordCountMap;
    
    public IntermediateRankingBolt() {
	_wordCountMap = new HashMap<String, Word>();
	_emitCounter = 0;
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
      this._collector = collector;      
    }
        
    @Override
    public void execute(Tuple tuple) {
	Integer emitCounter = (Integer) tuple.getValueByField("emitCounter");
	String word = (String) tuple.getValueByField("word");	
	if (emitCounter != _emitCounter) {	    
	    emitTopWords(emitCounter);	    
	} 
	processWord(word);	
	
    }

    private void processWord(String word) {
	if (_wordCountMap.containsKey(word)) {
	    _wordCountMap.get(word).incrementCount();
	} else {
	    _wordCountMap.put(word, new Word(word));
	}
	
    }

    private void emitTopWords(int newEmitCounter) {	
	List<Word> topWords = new ArrayList<Word>();
	int totalSeenWords = _wordCountMap.size();
	
	List<Word> words = new ArrayList<Word>();
	for (Entry<String, Word> entry : _wordCountMap.entrySet()) {
	    words.add(entry.getValue());
	}
	Collections.sort(words);
	
	for (int i = 0; i < totalSeenWords / 2; ++i) {
	    topWords.add(words.get(i));
	}
	
	_collector.emit(new Values(_emitCounter, topWords));
	_emitCounter = newEmitCounter;
	_wordCountMap.clear();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
	ofd.declare(new Fields("emitCounter", "topWords"));
    }    
}