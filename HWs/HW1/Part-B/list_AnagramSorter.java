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
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
	
	  public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	  	String line = value.toString();
	    StringTokenizer tokenizer = new StringTokenizer(line);
	    while (tokenizer.hasMoreTokens()) {
	    	word.set(tokenizer.nextToken());
	      output.collect(word, one);
	    }
	  }
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, MyArrayWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, MyArrayWritable> output, Reporter reporter) throws IOException {
    	//int sum = 0;
    	//while (values.hasNext()) {
    	//	sum += values.next().get();
    	//}
    	//output.collect(key, new IntWritable(sum));
			ArrayList<IntWritable> list = new ArrayList<IntWritable>();    
    	while ( values.hasNext()) {
				int val = values.next().get();
        list.add(new IntWritable(val));
    	}
			output.collect(key, new MyArrayWritable(IntWritable.class, list.toArray(new IntWritable[list.size()]) ) );
    	//context.write(key, new MyArrayWritable(IntWritable.class, list.toArray(new IntWritable[list.size()])));
  	}
	}

  public static void main(String[] args) throws Exception {
  	JobConf conf = new JobConf(AnagramSorter.class);
    conf.setJobName("AnagramSorter");

    conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(MyArrayWritable.class);

    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

		// Set Output and Input Parameters
    //job.setMapOutputKeyClass(Text.class);
    //job.setMapOutputValueClass(IntWritable.class);

    //job.setOutputKeyClass(Text.class);
    //job.setOutputValueClass(MyArrayWritable.class);
		
		// set IO value
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
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
