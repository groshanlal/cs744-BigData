package storm.starter.cs744Assignment2.Question2;

import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class EmitCounterRotationPolicy implements FileRotationPolicy {    
    int _emitCounter;

    public EmitCounterRotationPolicy() {
        _emitCounter = 0;
    }

    @Override
    public boolean mark(Tuple tuple, long offset) {	
	if (!tuple.contains("emitCounter")) {
	    return false;
	}
        int currentEmitCounter = (Integer) tuple.getValueByField(("emitCounter"));
        if (currentEmitCounter != _emitCounter) {
            _emitCounter = currentEmitCounter;
            return true;  // Rotate file now, as we have seen now a new emit counter.
        } else {
            return false;
        }
    }

    @Override
    public void reset() {
        // do nothing
    }

}