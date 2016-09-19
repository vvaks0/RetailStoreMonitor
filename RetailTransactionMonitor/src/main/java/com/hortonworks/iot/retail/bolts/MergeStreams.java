package com.hortonworks.iot.retail.bolts;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class MergeStreams extends BaseRichBolt {
	private OutputCollector collector;
	public void execute(Tuple tuple) {
		System.out.println("*********************************** Receiving Tuple from Stream: " + tuple.getSourceStreamId());
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("PotentialFraudStream", new Fields("EnrichedInventoryUpdate","ProvenanceEvent"));
	}

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		this.collector = collector;
	}
}
