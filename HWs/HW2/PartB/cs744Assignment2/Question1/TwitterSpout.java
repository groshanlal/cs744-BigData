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


import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.storm.Config;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.Nimbus.Client;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

@SuppressWarnings("serial")
public class TwitterSpout extends BaseRichSpout {

	SpoutOutputCollector _collector;
	Client _client;
	LinkedBlockingQueue<Status> _queue = null;	
	TwitterStream _twitterStream;
	String _consumerKey;
	String _consumerSecret;
	String _accessToken;
	String _accessTokenSecret;
	String[] _keyWords;
	String _executionMode;
	String _topologyName;
	int _tweetCounter;	
	
	final static int MAX_OUTPUT_TWEET_COUNT = 5000000;

	public TwitterSpout(String consumerKey, String consumerSecret,
			String accessToken, String accessTokenSecret, String[] keyWords, 
			String topologyName, String mode) {
		this._consumerKey = consumerKey;
		this._consumerSecret = consumerSecret;
		this._accessToken = accessToken;
		this._accessTokenSecret = accessTokenSecret;
		this._keyWords = keyWords;
		this._topologyName = topologyName;
		this._executionMode = mode;
		this._tweetCounter = 0;		
	}

	public TwitterSpout() {
		// TODO Auto-generated constructor stub
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_queue = new LinkedBlockingQueue<Status>(1000);
		_collector = collector;
		if (_executionMode.equals("cluster")) {
		    _client = NimbusClient.getConfiguredClient(conf).getClient();
		}

		StatusListener listener = new StatusListener() {

			@Override
			public void onStatus(Status status) {
			
				_queue.offer(status);
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice sdn) {
			}

			@Override
			public void onTrackLimitationNotice(int i) {
			}

			@Override
			public void onScrubGeo(long l, long l1) {
			}

			@Override
			public void onException(Exception ex) {
			}

			@Override
			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub

			}

		};

		_twitterStream = new TwitterStreamFactory(
				new ConfigurationBuilder().setJSONStoreEnabled(true).build())
				.getInstance();

		_twitterStream.addListener(listener);
		_twitterStream.setOAuthConsumer(_consumerKey, _consumerSecret);
		AccessToken token = new AccessToken(_accessToken, _accessTokenSecret);
		_twitterStream.setOAuthAccessToken(token);
		
		if (_keyWords.length == 0) {

			_twitterStream.sample();
		}

		else {

			//FilterQuery query = new FilterQuery().language(new String[]{"en"}).track(_keyWords);			
			FilterQuery query = new FilterQuery().track(_keyWords);			
			_twitterStream.filter(query);
		}

	}

	@Override
	public void nextTuple() {
		Status ret = _queue.poll();
		if (ret == null) {
			Utils.sleep(50);
		} else {
		    	// From the collected tweet, just emit the text. Also perform simple cleaning 
		    	// by replacing multiple whitespaces with a single whitespace.
		    	String tweet = ret.getText().replaceAll("\\s+", " ");
		    	_collector.emit(new Values(tweet));
		    	++_tweetCounter;		    	
			
			// Kill the cluster if the tweet counter exceeds the output counter.
			if (_executionMode.equals("cluster") && _tweetCounter >= MAX_OUTPUT_TWEET_COUNT) {			    
			    try {
				_client.killTopology(_topologyName);
			    } catch (NotAliveException e) {				
				e.printStackTrace();
			    } catch (AuthorizationException e) {				
				e.printStackTrace();
			    } catch (TException e) {
				e.printStackTrace();
			    }
			}

		}
	}

	@Override
	public void close() {
		_twitterStream.shutdown();
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

}