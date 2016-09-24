package com.hortonworks.iot.retail.bolts;

import java.util.HashMap;
import java.util.Map;

import org.cometd.client.BayeuxClient;
import org.cometd.client.transport.ClientTransport;
import org.cometd.client.transport.LongPollingTransport;
import org.eclipse.jetty.client.HttpClient;

import com.hortonworks.iot.retail.events.Product;
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

public class PublishInventoryUpdate extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private Constants constants;
	private BayeuxClient bayuexClient;
	private OutputCollector collector;
	
	public void execute(Tuple tuple) {
		@SuppressWarnings("unchecked")
		Map<String,Product> lostInventory = (Map<String,Product>) tuple.getValueByField("EnrichedInventoryUpdate");
		
		Map<String, Object> data = new HashMap<String, Object>();
		for (Map.Entry<String, Product> lostItem : lostInventory.entrySet()){
			data.put("productId", lostItem.getValue().getProductId());
			data.put("itemName", lostItem.getValue().getProductName());
			data.put("category", lostItem.getValue().getProductCategory());
			data.put("subCategory", lostItem.getValue().getProductSubCategory());
			data.put("price", Double.valueOf(lostItem.getValue().getPrice()));
			//data.put("latitude", lostItem.getLatitude());
			//data.put("longitude", lostItem.getLongitude());
			//data.put("brand", lostItem.getValue().getBrand());
		
			//bayuexClient.getChannel(constants.getFraudAlertChannel()).publish(data);
		}
		
		collector.ack(tuple);
	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		this.constants = new Constants();
		this.collector = collector;
		
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
