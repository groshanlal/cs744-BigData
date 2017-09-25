package org.myorg;

import java.io.*;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
  
public class AnagramSorter {
  

  public static class Anagram {
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
        //  sum += values.next().get();
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
  }

  public static class MySorter {
    public static class Map extends MapReduceBase implements Mapper<Text, Text, IntWritable, Text> {
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

    public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, NullWritable, Text> {
      public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        output.collect(NullWritable.get(), values);
      }
    } 
  }

  public static void main(String[] args) throws Exception {
    //Configuration conf = getConf();
    //Job job = new Job(conf, "AnagramSorter");
    JobConf job = new JobConf(AnagramSorter.class);
    job.setJobName("AnagramSorter");

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(Anagram.Map.class);
    //conf.setCombinerClass(Reduce.class);
    job.setReducerClass(Anagram.Reduce.class);

    // Set Output and Input Parameters
    //job.setMapOutputKeyClass(Text.class);
    //job.setMapOutputValueClass(IntWritable.class);

    //job.setOutputKeyClass(Text.class);
    //job.setOutputValueClass(MyArrayWritable.class);
    
    // set IO value
    job.setInputFormat(TextInputFormat.class);
    job.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    JobClient.runJob(job);
    //job.waitForCompletion(true);
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
