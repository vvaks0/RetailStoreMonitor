package com.hortonworks.iot.retail.bolts;

import java.util.Iterator;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

public class MergeStreams extends BaseWindowedBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	public void execute(TupleWindow tupleWindow) {
		Iterator<Tuple> tupleWindowIterator = tupleWindow.get().iterator();
		int i = 0;
		while(tupleWindowIterator.hasNext()){
			i++;
			System.out.println("*********************************** Receiving Tuple count " + i + "from Stream: " + tupleWindowIterator.next().getSourceStreamId());
		}
	}
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("PotentialFraudStream", new Fields("EnrichedInventoryUpdate","ProvenanceEvent"));
	}

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		this.collector = collector;
	}
}
