package org.myorg;

import java.io.*;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
	
public class AnagramSorter {
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private Text sorted = new Text();

	  public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	  	String line = value.toString();
	    StringTokenizer tokenizer = new StringTokenizer(line);
	    while (tokenizer.hasMoreTokens()) {
	    	word.set(tokenizer.nextToken());
				char[] chars = word.toString().toCharArray();
        Arrays.sort(chars);
				sorted.set(new String(chars));
	      output.collect(sorted, word);
	    }
	  }
	}
	
	public static class Reduce  extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    	//int sum = 0;
    	//while (values.hasNext()) {
    	//	sum += values.next().get();
    	//}
    	//output.collect(key, new IntWritable(sum));
			//ArrayList<IntWritable> list = new ArrayList<IntWritable>();    
      StringBuilder strVal = new StringBuilder();
    	while ( values.hasNext()) {
         
				strVal.append(values.next().toString());
				strVal.append(",");
    	}
			//output.collect(key, new MyArrayWritable(IntWritable.class, list.toArray(new IntWritable[list.size()]) ) );
			output.collect(key, new Text(strVal.toString()));
    	//context.write(key, new MyArrayWritable(IntWritable.class, list.toArray(new IntWritable[list.size()])));
  	}
	}
	
	public static class Map_sort extends MapReduceBase implements Mapper<Text, Text, IntWritable, Text> {
    //private final static IntWritable one = new IntWritable(1);
    private Text word = new Text(); 
    //private Text sorted = new Text(); 

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
        word.set(tokenizer.nextToken());
				
				int count = StringUtils.countMatches(word.toString(), ",");        
        output.collect(new IntWritable(count), word);
      }
    }
  }


	public static class Reduce_sort  extends MapReduceBase implements Reducer<IntWritable, Text, NullWritable, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      output.collect(NullWritable.get(), values);
    }
  }	


  public static void main(String[] args) throws Exception {
  	JobConf conf = new JobConf(AnagramSorter.class);
    conf.setJobName("AnagramSorter");
		
		// set MAP output parameters
    conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		// set RDC output paramters
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(Map.class);
    //conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);
		
		// set IO value
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path("inter_dir/output/"));

    JobClient.runJob(conf);

  	/*
   	* Sorting
   	*/
		JobConf conf1 = new JobConf(AnagramSorter.class);
    conf1.setJobName("AnagramSorter");

    // set MAP output parameters
    conf1.setMapOutputKeyClass(IntWritable.class);
    conf1.setMapOutputValueClass(Text.class);
    // set RDC output paramters
    conf1.setOutputKeyClass(NullWritable.class);
    conf1.setOutputValueClass(Text.class);

    conf1.setMapperClass(Map_sort.class);
    //conf.setCombinerClass(Reduce.class);
    conf1.setReducerClass(Reduce_sort.class);

    // set IO value
    conf1.setInputFormat(TextInputFormat.class);
    conf1.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path("inter_dir/output/"));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);

  	
	}
}

class MyArrayWritable extends ArrayWritable{

	public MyArrayWritable(Class<? extends Writable> valueClass, Writable[] values) {
    super(valueClass, values);
	}
	public MyArrayWritable(Class<? extends Writable> valueClass) {
    super(valueClass);
	}

	@Override
	public IntWritable[] get() {
    return (IntWritable[]) super.get();
	}

	@Override
  public void write(DataOutput arg0) throws IOException {
  	System.out.println("write method called");
  	super.write(arg0);
	}
  
  @Override
	public String toString() {
  	return Arrays.toString(get());
	}

}
