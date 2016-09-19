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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.hortonworks.iot.retail.events.EnrichedInventoryUpdate;
import com.hortonworks.iot.retail.events.EnrichedTransaction;
import com.hortonworks.iot.retail.events.IncomingTransaction;
import com.hortonworks.iot.retail.events.InventoryUpdate;
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


public class EnrichInventoryUpdate extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private String componentId;
	private String componentType;
	private OutputCollector collector;
	private Constants constants;
	private HTable productTable = null;
	private HTable locationTable = null;
	private Connection conn = null;
	
	public void execute(Tuple tuple) {
		InventoryUpdate inventoryUpdate = (InventoryUpdate) tuple.getValueByField("InventoryUpdate");
		EnrichedInventoryUpdate enrichedInventoryUpdate = new EnrichedInventoryUpdate();
		
		//String actionType = "MODIFY";
		//List<StormProvenanceEvent> stormProvenance = (List<StormProvenanceEvent>)tuple.getValueByField("ProvenanceEvent");
		//String transactionKey = stormProvenance.get(0).getEventKey();
		
		//StormProvenanceEvent provenanceEvent = new StormProvenanceEvent(transactionKey, actionType, componentId, componentType);
	    //stormProvenance.add(provenanceEvent);
		
	    enrichedInventoryUpdate.setProductId(inventoryUpdate.getProductId());
	    enrichedInventoryUpdate.setLocationId(inventoryUpdate.getLocationId());
		
		//System.out.println("********************** Enriching InventoryUpdate: " + transactionKey);	    
	    ResultSet resultSet = null;
	    Boolean matchedProduct = false;
	    Boolean matchedLocation = false;
		try {
			List<Product> products = new ArrayList<Product>();
			resultSet = conn.createStatement().executeQuery("SELECT * FROM \"Product\" WHERE \"productId\" = '" + enrichedInventoryUpdate.getProductId() + "')");
			while (resultSet.next()) {
		    	System.out.println("******************** " + 
    					resultSet.getString("productId") + "," +
    					resultSet.getString("productCategory") + "," +
    					resultSet.getString("productSubCategory") + "," +
    					resultSet.getString("manufacturer") + "," +
    					resultSet.getString("productName") + "," + 
    					resultSet.getDouble("price"));
		    	products.add(new Product(
		    			resultSet.getString("productId"),
		    			resultSet.getString("productCategory"),
		    			resultSet.getString("productSubCategory"),
		    			resultSet.getString("manufacturer"),
		    			resultSet.getString("productName"), 
		    			resultSet.getDouble("price")));
		    	matchedProduct = true;
			}
			
		    resultSet = conn.createStatement().executeQuery("SELECT * FROM \"Location\" WHERE \"locationId\" = '" + enrichedInventoryUpdate.getLocationId() + "'");
		    while (resultSet.next()) {
		    	System.out.println(resultSet.getString(1) + " " + resultSet.getString(2));
		    	enrichedInventoryUpdate.setLocationId(resultSet.getString("locationId"));
		    	enrichedInventoryUpdate.setStreetAddress(resultSet.getString("address"));
		    	enrichedInventoryUpdate.setCity(resultSet.getString("city"));
		    	enrichedInventoryUpdate.setState(resultSet.getString("state"));
		    	enrichedInventoryUpdate.setZipCode(resultSet.getString("zip"));
		    	enrichedInventoryUpdate.setLatitude(resultSet.getString("latitude"));
		    	enrichedInventoryUpdate.setLongitude(resultSet.getString("longitude"));
		    	enrichedInventoryUpdate.setBrand(resultSet.getString("brand"));
		    	matchedLocation = true;
		    }
		    
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		if(matchedLocation && matchedProduct){
			//actionType = "SEND";
			//provenanceEvent = new StormProvenanceEvent(transactionKey, actionType, componentId, componentType);
			//provenanceEvent.setTargetDataRepositoryType("HBASE");
			//provenanceEvent.setTargetDataRepositoryLocation(constants.getZkConnString() + ":" + constants.getZkHBasePath() + ":" + constants.getZkHBasePath());
			//stormProvenance.add(provenanceEvent);
			
			//persistTransactionToHbase(enrichedInventoryUpdate);
			collector.emit(tuple, new Values((EnrichedInventoryUpdate)enrichedInventoryUpdate));
			collector.ack(tuple);
		}
		else{
			System.out.println("The InventoryUpdate refers to a Product and/or Location that are not in the data store.");
			System.out.println("ProductId: " + enrichedInventoryUpdate.getProductId());
			collector.ack(tuple);
		}
	}
	
	public void persistTransactionToHbase(EnrichedTransaction transaction){
		try {
			conn.createStatement().executeUpdate("UPSERT INTO \"TransactionHistory\" VALUES('" + 
					transaction.getTransactionId() + "','" + 
					transaction.getLocationId() + "','" + 
					transaction.getAccountNumber() + "','" + 
					transaction.getAccountType() + "'," + 
					transaction.getAmount() + ",'" + 
					transaction.getCurrency() + "','" + 
					transaction.getIsCardPresent() + "'," + 
					transaction.getTransactionTimeStamp() + ")");
			conn.commit();
			
			List<Product> products = transaction.getProducts();
			Iterator<Product> iterator = products.iterator();
			Product currentProduct= new Product();
			String transactionItemsUpsert = "";
			while(iterator.hasNext()){
				currentProduct = iterator.next();
				transactionItemsUpsert = transactionItemsUpsert + " UPSERT INTO \"TransactionItems\" VALUES('" +
					transaction.getTransactionId() + currentProduct.getProductId() + "','" +
					transaction.getTransactionId() + "','" +
					currentProduct.getProductId() + "') \n";
			}
			conn.createStatement().executeUpdate(transactionItemsUpsert);
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
				System.out.println("******************** EnrichInventoryUpdate prepare() Waiting for Phoenix Tables to be created..."); 
			}
			System.out.println("******************** EnrichInventoryUpdate prepare() Phoenix Tables are ready...");
			hbaseAdmin.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		this.collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("InventoryStream", new Fields("EnrichedInventoryUpdate","ProvenanceEvent"));
	}
}