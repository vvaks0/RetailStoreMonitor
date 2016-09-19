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
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import com.hortonworks.iot.retail.events.EnrichedInventoryUpdate;
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
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

public class TransactionMonitor extends BaseWindowedBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private String componentId;
	private String componentType;
	private Constants constants;
	private Connection conn = null;
	private HTable transactionHistoryTable = null;
	private List<EnrichedTransaction> purchaseTransactions = new ArrayList<EnrichedTransaction>();
	private Map<String,EnrichedInventoryUpdate> inventoryUpdates = new HashMap<String, EnrichedInventoryUpdate>();
	
	public void execute(TupleWindow tupleWindow)  {
		String actionType;
		String transactionKey;
		Tuple tuple;
		EnrichedInventoryUpdate enrichedInventoryUpdate;
		EnrichedTransaction transaction;
		List<StormProvenanceEvent> stormProvenance;
		StormProvenanceEvent provenanceEvent;
		
		System.out.println("******************* TransactionMonitorBolt execute() Evaluating Window of Transactions and InventoryUpdates...");
		Iterator<Tuple> tupleWindowIterator = tupleWindow.get().iterator();
		while(tupleWindowIterator.hasNext()){
			tuple = tupleWindowIterator.next();
			
			if(tuple.getSourceStreamId().equalsIgnoreCase("TransactionStream")){			
				actionType = "MODIFY";
				transaction = (EnrichedTransaction) tuple.getValueByField("EnrichedTransaction");
				stormProvenance = (List<StormProvenanceEvent>)tuple.getValueByField("ProvenanceEvent");
				transactionKey = stormProvenance.get(0).getEventKey();
				provenanceEvent = new StormProvenanceEvent(transactionKey, actionType, componentId, componentType);
				stormProvenance.add(provenanceEvent);
	    
				purchaseTransactions.add(transaction);
				
				collector.emit("ProvenanceRegistrationStream", new Values(stormProvenance));
				collector.ack(tuple);
			}else if(tuple.getSourceStreamId().equalsIgnoreCase("InventoryStream")){
				enrichedInventoryUpdate = (EnrichedInventoryUpdate)tuple.getValueByField("EnrichedInventoryUpdate");
				inventoryUpdates.put(enrichedInventoryUpdate.getProductId(), enrichedInventoryUpdate);
				collector.ack(tuple);
			}
		}
		System.out.println("******************* TransactionMonitorBolt execute() Received " + purchaseTransactions.size() + 
				" PurchaseTransactions and " + inventoryUpdates.size() + " InventoryUpdates");
		Product currentProduct;
		Iterator<EnrichedTransaction> currentTransactionsIterator = purchaseTransactions.iterator();
		Iterator<Product> transactionProductsIterator;
		System.out.println("******************* TransactionMonitorBolt execute() Looking for InventoryUpdates without accompanying PurchaseTransactions...");
		while(currentTransactionsIterator.hasNext()){
			transaction = currentTransactionsIterator.next();
			transactionProductsIterator = transaction.getProducts().iterator();
			while(transactionProductsIterator.hasNext()){
				currentProduct = transactionProductsIterator.next();
				if(inventoryUpdates.containsKey(currentProduct.getProductId())){
					inventoryUpdates.remove(currentProduct.getProductId());
				}
			}
		}
		if(inventoryUpdates.size() > 0){
			System.out.println("******************* TransactionMonitorBolt execute() Detected " + inventoryUpdates.size() + "InventoryUpdates without an accompanying PurchaseTransaction");
			collector.emit("PotentialTheftStream", new Values(inventoryUpdates));
		}else{
			System.out.println("******************* TransactionMonitorBolt execute() All InventoryUpdates were associated with a PurchaseTransaction");
		}
	}
	
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
		declarer.declareStream("PotentialTheftStream", new Fields("InventoryUpdates"));
		declarer.declareStream("ProvenanceRegistrationStream", new Fields("ProvenanceEvent"));
	}
}