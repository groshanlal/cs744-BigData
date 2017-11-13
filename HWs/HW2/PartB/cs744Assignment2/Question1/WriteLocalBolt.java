/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.starter.cs744Assignment2.Question1;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class WriteLocalBolt extends BaseBasicBolt {   
    private String outputFilepath;    
    private int tweetCounter;
    
    public WriteLocalBolt(String outputFilepath) {
	this.outputFilepath = outputFilepath;
	File outputFile = new File(outputFilepath);
	try {
	    Files.deleteIfExists(outputFile.toPath());
	} catch (IOException e) {
	    e.printStackTrace();
	}
	tweetCounter = 0;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
	String tweet = (String) tuple.getValueByField("tweet");	
	try {
	    BufferedWriter localFileWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFilepath, true),"UTF-8"));	    
	    localFileWriter.write(tweet);
	    localFileWriter.newLine();
	    localFileWriter.close();
	} catch (IOException e) {
	    e.printStackTrace();
	}	
	System.out.println("Curret processed tweet count = " + (++tweetCounter));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }
        
    public String getOutputFilepath() {
	return outputFilepath;
    }

    public void setOutputFilepath(String outputFilepath) {
	this.outputFilepath = outputFilepath;
    }

}