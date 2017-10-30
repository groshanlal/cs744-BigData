package storm.starter.cs744Assignment2.Question2;
import java.util.Map;

import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.task.TopologyContext;

/**
 * Creates file names with the following format:
 * <pre>
 *     {tweets/topWords}-{rotationNum}{extension}
 * </pre>
 * For example:
 * <pre>
 *     tweets-1.txt
 *     topWords-1.txt
 * </pre>
 *
 */
@SuppressWarnings("serial")
public class OutputFileNameFormat implements FileNameFormat {
    private String path = "/storm";
    private String prefix = "";
    private String extension = ".txt";

    /**
     * Overrides the default prefix.
     *
     * @param prefix
     * @return
     */
    public OutputFileNameFormat withPrefix(String prefix){
        this.prefix = prefix;
        return this;
    }

    /**
     * Overrides the default file extension.
     *
     * @param extension
     * @return
     */
    public OutputFileNameFormat withExtension(String extension){
        this.extension = extension;
        return this;
    }

    public OutputFileNameFormat withPath(String path){
        this.path = path;
        return this;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map conf, TopologyContext topologyContext) {
        
    }

    @Override
    public String getName(long rotation, long timeStamp) {
        return this.prefix + "-" + formatRotation(rotation) + this.extension;
    }

    public String getPath(){
        return this.path;
    }
    
    private String formatRotation(long rotation) {
    StringBuilder sb = new StringBuilder();
    if (rotation <= 9) {
        sb.append("000" + rotation);
    } else if (rotation <= 99) {
        sb.append("00" + rotation);
    } else if (rotation <= 999){
        sb.append("0" + rotation);
    } else {
        sb.append(rotation);
    }
    return sb.toString();
    }
}