package com.hortonworks.iot.retail.bolts;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.hortonworks.iot.retail.events.EnrichedTransaction;
import com.hortonworks.iot.retail.events.IncomingTransaction;
import com.hortonworks.iot.retail.events.Product;
import com.hortonworks.iot.retail.events.SocialMediaEvent;
import com.hortonworks.iot.retail.util.Constants;
import com.hortonworks.iot.retail.util.StormProvenanceEvent;

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


public class ProcessSocialMediaEvent extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private String componentId;
	private String componentType;
	private OutputCollector collector;
	private Constants constants;
	private Connection conn = null;
	
	public void execute(Tuple tuple) {
		SocialMediaEvent socialMediaEvent = (SocialMediaEvent) tuple.getValueByField("SocialMediaEvent");
			
			persistTransactionToHbase(socialMediaEvent);
			System.out.println("********************** ProcessMediaEvent execute() emitting Tuple");
			collector.emit(tuple, new Values((SocialMediaEvent)socialMediaEvent));
			collector.ack(tuple);
	}
	
	public void persistTransactionToHbase(SocialMediaEvent event){
		try {
			conn.createStatement().executeUpdate("UPSERT INTO \"SocialMediaEvnets\" VALUES('" + 
					event.getEventTimeStamp() + "','" + 
					event.getStatement() + "','" + 
					event.getIpAddress() + "','" + 
					event.getLatitude() + "'," + 
					event.getLongitude() + ",'" + 
					event.getSentiment() + "')");
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("deprecation")
	public void prepare(Map arg0, TopologyContext context, OutputCollector collector) {
		this.constants = new Constants();
		this.componentId = context.getThisComponentId();
		this.componentType = "BOLT";
		
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", constants.getZkHost());
		config.set("hbase.zookeeper.property.clientPort", constants.getZkPort());
		config.set("zookeeper.znode.parent", constants.getZkHBasePath());
			
		try {
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
			conn = DriverManager.getConnection("jdbc:phoenix:"+ constants.getZkHost() + ":" + constants.getZkPort() + ":" + constants.getZkHBasePath());
		
			HBaseAdmin hbaseAdmin;
			hbaseAdmin = new HBaseAdmin(config);
			while(!hbaseAdmin.tableExists("SocialMediaEvents")){
				Thread.sleep(1000);
				System.out.println("******************** ProcessMediaEvent prepare() Waiting for Phoenix Tables to be created..."); 
			}
			System.out.println("******************** ProcessMediaEvent prepare() Phoenix Tables created...");
			hbaseAdmin.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		System.out.println("******************** ProcessMediaEvent prepare() Phoenix Tables are ready");
		this.collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("SocialMediaStream", new Fields("SocialMediaEvent"));
	}
}