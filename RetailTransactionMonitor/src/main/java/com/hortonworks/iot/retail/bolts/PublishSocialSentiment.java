package com.hortonworks.iot.retail.bolts;

import java.util.HashMap;
import java.util.Map;

import org.cometd.client.BayeuxClient;
import org.cometd.client.transport.ClientTransport;
import org.cometd.client.transport.LongPollingTransport;
import org.eclipse.jetty.client.HttpClient;

import com.hortonworks.iot.retail.events.EnrichedTransaction;
import com.hortonworks.iot.retail.events.SocialMediaEvent;
import com.hortonworks.iot.retail.util.Constants;

/*
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
*/

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


public class PublishSocialSentiment extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private BayeuxClient bayuexClient;
	private OutputCollector collector;
	private Constants constants;
	
	public void execute(Tuple tuple) {
		SocialMediaEvent socialMediaEvent = (SocialMediaEvent) tuple.getValueByField("SocialMediaEvent");
		
		Map<String, Object> data = new HashMap<String, Object>();
		
		data.put("transactionTimeStamp", socialMediaEvent.getEventTimeStamp());
		data.put("region", socialMediaEvent.getRegion());
		data.put("latitude", socialMediaEvent.getLatitude());
		data.put("longitude", socialMediaEvent.getLongitude());
		data.put("sentiment", Double.valueOf(socialMediaEvent.getSentiment()));
		
		bayuexClient.getChannel(constants.getSocialMediaChannel()).publish(data);
		
		//collector.emit(tuple, new Values((SocialMediaEvent)socialMediaEvent));
		collector.ack(tuple);
	}

	public void prepare(Map arg0, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.constants = new Constants();
		HttpClient httpClient = new HttpClient();
		try {
			httpClient.start();
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Prepare the transport
		Map<String, Object> options = new HashMap<String, Object>();
		ClientTransport transport = new LongPollingTransport(options, httpClient);

		// Create the BayeuxClient
		bayuexClient = new BayeuxClient(constants.getPubSubUrl(), transport);
		
		bayuexClient.handshake();
		boolean handshaken = bayuexClient.waitFor(3000, BayeuxClient.State.CONNECTED);
		if (handshaken)
		{
			System.out.println("Connected to Cometd Http PubSub Platform");
		}
		else{
			System.out.println("Could not connect to Cometd Http PubSub Platform");
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("EnrichedTransaction"));
	}
}