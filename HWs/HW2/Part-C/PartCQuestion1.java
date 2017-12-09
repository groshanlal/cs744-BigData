package org.myorg.quickstart;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;


public class PartCQuestion1{

	public static void main(String[] args) throws Exception{

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		DataStream<Tuple4<Integer, Integer, Long, String>> ds = env.addSource(DataSource.create());
		
		DataStream<Tuple4<Integer, Integer, Long, String>> msds = ds.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Integer, Integer, Long, String>>(){
	@Override
	public long extractAscendingTimestamp(Tuple4<Integer, Integer, Long, String> record){
		long time = record.getField(2);
		time = time*1000;
		return time;
	}	
});
		//ds.timeWindowAll(Time.seconds(60))
                  //.keyBy(3);
		//DataStream<Tuple4<Integer, Integer, Long, String>> winds = ds.timeWindowAll(Time.seconds(60)
		int slide_length = Integer.parseInt(args[0]);
		String file_name = args[1];
		if(slide_length == 0){
			 DataStream<Tuple5<Long,Long,Integer, Integer, Integer>> inter = msds.timeWindowAll(Time.seconds(60))
                  		.apply(new AllWindowFunction<Tuple4<Integer, Integer, Long, String>, Tuple5<Long, Long,Integer, Integer, Integer>, TimeWindow>(){
                        @Override
                        public void apply(TimeWindow window,
                                Iterable<Tuple4<Integer, Integer, Long, String>> values,
                                Collector<Tuple5<Long, Long,Integer, Integer, Integer>> out) throws Exception {
                                int sumMT = 0;
                                int sumRT = 0;
                                int sumRE = 0;
                                for(Tuple4<Integer, Integer, Long, String> t: values){
                                        if(t.f3.compareTo("MT")==0)
                                        sumMT += 1;
                                        else if (t.f3.compareTo("RT")==0)
                                                sumRT += 1;
                                        else
                                                sumRE += 1;
                                }
                        out.collect(new Tuple5<Long, Long, Integer, Integer, Integer>(window.getStart(),window.getEnd(),sumMT,sumRT,sumRE));
                        }
		});


                DataStream<String> result = inter.flatMap(new FlatMapFunction<Tuple5<Long, Long,Integer, Integer, Integer>,String>(){
                        @Override
                        public void flatMap(Tuple5<Long, Long,Integer, Integer,Integer> value, Collector<String> out) throws Exception{
                                if(value.f2 >= 100){

                                        String stringMT =  "TimeWindow{start="+value.f0.toString()+", end="+value.f1.toString()+"} Count:"+value.f2.toString()+" Type: MT";
                                        out.collect(stringMT);
				}
                                if(value.f3 >= 100){

                                        String stringRT =  "TimeWindow{start="+value.f0.toString()+", end="+value.f1.toString()+"} Count:"+value.f3.toString()+" Type: RT";
                                        out.collect(stringRT);
				}
                                if(value.f4 >= 100){

                                        String stringRE =  "TimeWindow{start="+value.f0.toString()+", end="+value.f1.toString()+"} Count:"+value.f4.toString()+" Type: RE";
                                        out.collect(stringRE);
				}

                        }

		});
		result.print();
		result.writeAsText(file_name,WriteMode.OVERWRITE);
		}else {
		DataStream<Tuple5<Long,Long,Integer, Integer, Integer>> inter = msds
		  .timeWindowAll(Time.seconds(60), Time.seconds(slide_length))//.timeWindowAll(Time.seconds(60))
		  .apply(new AllWindowFunction<Tuple4<Integer, Integer, Long, String>, Tuple5<Long, Long,Integer, Integer, Integer>, TimeWindow>(){
			@Override
			public void apply(TimeWindow window,
				Iterable<Tuple4<Integer, Integer, Long, String>> values,
				Collector<Tuple5<Long, Long,Integer, Integer, Integer>> out) throws Exception {
				int sumMT = 0;
				int sumRT = 0;
				int sumRE = 0;
				for(Tuple4<Integer, Integer, Long, String> t: values){
					if(t.f3.compareTo("MT")==0)
					sumMT += 1;
					else if (t.f3.compareTo("RT")==0)
						sumRT += 1;
					else
						sumRE += 1;
				}
			out.collect(new Tuple5<Long, Long, Integer, Integer, Integer>(window.getStart(),window.getEnd(),sumMT,sumRT,sumRE));
			}
});


		DataStream<String> result = inter.flatMap(new FlatMapFunction<Tuple5<Long, Long,Integer, Integer, Integer>,String>(){
			@Override
			public void flatMap(Tuple5<Long, Long,Integer, Integer,Integer> value, Collector<String> out) throws Exception{
				if(value.f2 >= 100){

				 	String stringMT =  "TimeWindow{start="+value.f0.toString()+", end="+value.f1.toString()+"} Count:"+value.f2.toString()+" Type: MT";
					out.collect(stringMT);
}
				if(value.f3 >= 100){

				 	String stringRT =  "TimeWindow{start="+value.f0.toString()+", end="+value.f1.toString()+"} Count:"+value.f3.toString()+" Type: RT";
					out.collect(stringRT);
}
				if(value.f4 >= 100){

				 	String stringRE =  "TimeWindow{start="+value.f0.toString()+", end="+value.f1.toString()+"} Count:"+value.f4.toString()+" Type: RE";
					out.collect(stringRE);
}

			}



});	
		result.print();	
		result.writeAsText(file_name,WriteMode.OVERWRITE);								//.timeWindow(Time.seconds(60));
        	//DataStream<Tuple4<Integer, Integer, Long, String>> ds = env.addSource(new DataSource());
                //DataStream<Tuple4<Integer, Integer, Long, String>> ds = env.addSource(new DataSource());
                //String outputpath = "/home/ubuntu/flinktest/result.txt";
		//inter.writeAsText(outputpath);
		//result.writeAsText(outputpath);
		//DataStream<String> result = 
/*
		msds.timeWindowAll(Time.seconds(60))
		.apply(new AllWindowFunction<Tuple4<Integer, Integer, Long, String>, String,  TimeWindow>(){
		public void apply(TimeWindow window, Iterable<Tuple4<Integer, Integer, Long, String>> values, Collector<String> out) throws Exception {
			Integer sum = 0;
			String type = "";

			for(Tuple4<Integer, Integer, Long, String> t : values){
				sum++;
				type = t.f3;
			}

                        if(sum >= 100){
				out.collect("TimeWindow{start=" + window.getStart() + ", end=" + window.getEnd()
+ "} Count:" + sum + " Type: " + type);//.writeAsText("/home/ubuntu/flinktest/result1.txt");
			}

		}
}).writeAsText("/home/ubuntu/flinktest/result2030.txt");

*/		}
		//result.writeAsText("/home/ubuntu/Assignment2PCQ1.txt",WriteMode.OVERWRITE);
		//msds.print();
		env.execute("Twitter Type Analysis");
	}

// A nested private class
        private static class DataSource extends RichSourceFunction<Tuple4<Integer, Integer, Long, String>> {

	private volatile boolean running = true;
	private final String filename = "/home/ubuntu/data/higgs-activity_time.txt";

	private DataSource() {

	}

	public static DataSource create() {
		return new DataSource();
	}

	@Override
	public void run(SourceContext<Tuple4<Integer, Integer, Long, String>> ctx) throws Exception {

		try{
			final File file = new File(filename);
			final BufferedReader br = new BufferedReader(new FileReader(file));

			String line = "";

			System.out.println("Start read data from \"" + filename + "\"");
			long count = 0L;
			while(running && (line = br.readLine()) != null) {
				if ((count++) % 10 == 0) {
					Thread.sleep(1);
				}
				ctx.collect(genTuple(line));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void cancel() {
		running = false;
	}

	private Tuple4<Integer, Integer, Long, String> genTuple(String line) {
		String[] item = line.split(" ");
		Tuple4<Integer, Integer, Long, String> record = new Tuple4<>();

		record.setField(Integer.parseInt(item[0]), 0);
		record.setField(Integer.parseInt(item[1]), 1);
		record.setField(Long.parseLong(item[2]), 2);
		record.setField(item[3], 3);

		return record;
	}
	}
} 
