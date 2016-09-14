package com.hortonworks.iot.retail.bolts;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import com.hortonworks.iot.retail.events.EnrichedTransaction;
import com.hortonworks.iot.retail.events.Product;
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

public class TransactionMonitor extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private String componentId;
	private String componentType;
	private Constants constants;
	private Connection conn = null;
	private HTable transactionHistoryTable = null;
	
	public void execute(Tuple tuple)  {
		EnrichedTransaction transaction = (EnrichedTransaction) tuple.getValueByField("EnrichedTransaction");
		
		String actionType = "SEND";
		List<StormProvenanceEvent> stormProvenance = (List<StormProvenanceEvent>)tuple.getValueByField("ProvenanceEvent");
		String transactionKey = stormProvenance.get(0).getEventKey();
	    StormProvenanceEvent provenanceEvent = new StormProvenanceEvent(transactionKey, actionType, componentId, componentType);
	    provenanceEvent.setTargetDataRepositoryType("HBASE");
	    provenanceEvent.setTargetDataRepositoryLocation(constants.getZkConnString() + ":" + constants.getZkHBasePath() + ":" + transactionHistoryTable.getName().getNameAsString());
	    stormProvenance.add(provenanceEvent);
	    
	    persistTransactionToHbase(transaction);
	    
		collector.emit("ProvenanceRegistrationStream", new Values(stormProvenance));
		collector.ack(tuple);
	}
	
	public void persistTransactionToHbase(EnrichedTransaction transaction){
		try {
			conn.createStatement().executeUpdate("UPSERT INTO TransactionHistory VALUES('" + 
					transaction.getTransactionId() + "','" + 
					transaction.getLocationId() + "','" + 
					transaction.getAccountNumber() + "','" + 
					transaction.getAccountType() + "','" + 
					transaction.getAmount() + "','" + 
					transaction.getCurrency() + "','" + 
					transaction.getIsCardPresent() + "'" + 
					transaction.getTransactionTimeStamp() + "')");
			
			List<Product> products = transaction.getProducts();
			Iterator<Product> iterator = products.iterator();
			Product currentProduct= new Product();
			String transactionItemsUpsert = "";
			while(iterator.hasNext()){
				currentProduct = iterator.next();
				transactionItemsUpsert = transactionItemsUpsert + " UPSERT INTO TransactionItems VALUES('" +
					transaction.getTransactionId() + currentProduct.getProductId() + "','" +
					transaction.getTransactionId() + "','" +
					currentProduct.getProductId() + "') \n";
			}
			conn.createStatement().executeUpdate(transactionItemsUpsert);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("deprecation")
	public void prepare(Map arg0, TopologyContext context, OutputCollector collector) {
		this.constants = new Constants();
		this.componentId = context.getThisComponentId();
		this.componentType = "BOLT";
		this.collector = collector;
		
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", constants.getZkHost());
		config.set("hbase.zookeeper.property.clientPort", constants.getZkPort());
		config.set("zookeeper.znode.parent", constants.getZkHBasePath());
		
	    // Instantiating HTable
		try {
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
			conn = DriverManager.getConnection("jdbc:phoenix:"+ constants.getZkHost() + ":" + constants.getZkPort() + ":" + constants.getZkHBasePath());
			HBaseAdmin hbaseAdmin = new HBaseAdmin(config);
			
			while(!hbaseAdmin.tableExists("TransactionHistory") 
					  && !hbaseAdmin.tableExists("TransactionItem") 
					  && !hbaseAdmin.tableExists("Product") 
					  && !hbaseAdmin.tableExists("Location")){
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				System.out.println("******************** TransactionMonitor prepare() Waiting for Phoenix Tables to be prepared..."); 
			}
						
			hbaseAdmin.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("LegitimateTransactionStream", new Fields("EnrichedTransaction"));
		declarer.declareStream("FraudulentTransactionStream", new Fields("EnrichedTransaction"));
		declarer.declareStream("ProvenanceRegistrationStream", new Fields("ProvenanceEvent"));
	}
}