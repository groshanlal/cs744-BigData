package storm.starter.cs744Assignment2.Question2;

import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class EmitRecordFormat implements RecordFormat {
    public static final String DEFAULT_FIELD_DELIMITER = ",";
    public static final String DEFAULT_RECORD_DELIMITER = "\n";
    private String fieldDelimiter = DEFAULT_FIELD_DELIMITER;
    private String recordDelimiter = DEFAULT_RECORD_DELIMITER;
    private Fields fields = null;

    /**
     * Only output the specified fields.
     *
     * @param fields
     * @return
     */
    public EmitRecordFormat withFields(Fields fields){
        this.fields = fields;
        return this;
    }

    /**
     * Overrides the default field delimiter.
     *
     * @param delimiter
     * @return
     */
    public EmitRecordFormat withFieldDelimiter(String delimiter){
        this.fieldDelimiter = delimiter;
        return this;
    }

    /**
     * Overrides the default record delimiter.
     *
     * @param delimiter
     * @return
     */
    public EmitRecordFormat withRecordDelimiter(String delimiter){
        this.recordDelimiter = delimiter;
        return this;
    }

    @Override
    public byte[] format(Tuple tuple) {
        StringBuilder sb = new StringBuilder();
        Fields fields = this.fields == null ? tuple.getFields() : this.fields;
        int size = fields.size();
        for(int i = 0; i < size; i++){
            String tupleValue = (String) tuple.getValueByField(fields.get(i)).toString();            
            if (tupleValue.trim().length() == 0) return "".getBytes();  // Do not output empty values.
            sb.append(tupleValue);
            if(i != size - 1){
                sb.append(this.fieldDelimiter);
            }
        }
        sb.append(this.recordDelimiter);
        return sb.toString().getBytes();
    }
}