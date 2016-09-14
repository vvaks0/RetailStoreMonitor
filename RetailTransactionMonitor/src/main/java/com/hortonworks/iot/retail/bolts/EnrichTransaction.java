package com.hortonworks.iot.retail.bolts;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.hortonworks.iot.retail.events.EnrichedTransaction;
import com.hortonworks.iot.retail.events.IncomingTransaction;
import com.hortonworks.iot.retail.events.Product;
import com.hortonworks.iot.retail.util.Constants;
import com.hortonworks.iot.retail.util.StormProvenanceEvent;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
*/

public class EnrichTransaction extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private String componentId;
	private String componentType;
	private OutputCollector collector;
	private Constants constants;
	private HTable productTable = null;
	private HTable locationTable = null;
	private HTable transactionHistoryTable = null;
	private HTable transactionItemsTable = null;
	private Connection conn = null;
	
	public void execute(Tuple tuple) {
		IncomingTransaction incomingTransaction = (IncomingTransaction) tuple.getValueByField("IncomingTransaction");
		EnrichedTransaction enrichedTransaction = new EnrichedTransaction();
		
		String actionType = "MODIFY";
		List<StormProvenanceEvent> stormProvenance = (List<StormProvenanceEvent>)tuple.getValueByField("ProvenanceEvent");
		String transactionKey = stormProvenance.get(0).getEventKey();
		
		StormProvenanceEvent provenanceEvent = new StormProvenanceEvent(transactionKey, actionType, componentId, componentType);
	    stormProvenance.add(provenanceEvent);
		
		enrichedTransaction.setAccountNumber(incomingTransaction.getAccountNumber());
		enrichedTransaction.setAccountType(incomingTransaction.getAccountType());
		enrichedTransaction.setAmount(incomingTransaction.getAmount());
		enrichedTransaction.setCurrency(incomingTransaction.getCurrency());
		enrichedTransaction.setIsCardPresent(incomingTransaction.getIsCardPresent());
		enrichedTransaction.setTransactionId(incomingTransaction.getTransactionId());
		enrichedTransaction.setTransactionTimeStamp(incomingTransaction.getTransactionTimeStamp());
		enrichedTransaction.setIpAddress(incomingTransaction.getIpAddress());
		
		System.out.println("********************** Enriching event: " + transactionKey);	    
	    Result result = null;
	    ResultSet resultSet = null;
		try {
			//result = productTable.get(get);
			List<Product> products = new ArrayList<Product>();
			resultSet = conn.createStatement().executeQuery("SELECT * FROM Product WHERE productId IN ('" + String.join("','", incomingTransaction.getItems()) + "')");
			while (resultSet.next()) {
		    	System.out.println(resultSet.getString(1) + " " + resultSet.getString(2) + " " + resultSet.getString(3) + " " + resultSet.getString(4)+ " " + resultSet.getString(5) + " " + resultSet.getString(6));
		    	products.add(new Product(resultSet.getString(1),resultSet.getString(2),resultSet.getString(3),resultSet.getString(4),resultSet.getString(5), resultSet.getDouble(6)));
		    }
			
		    resultSet = conn.createStatement().executeQuery("SELECT * FROM Location WHERE locationId = '" + incomingTransaction.toString() + "'");
		    while (resultSet.next()) {
		    	System.out.println(resultSet.getString(1) + " " + resultSet.getString(2));
		    	enrichedTransaction.setLocationId(resultSet.getString(1));
		    	enrichedTransaction.setStreetAddress(resultSet.getString(2));
		    	enrichedTransaction.setCity(resultSet.getString(3));
		    	enrichedTransaction.setState(resultSet.getString(4));
		    	enrichedTransaction.setZipCode(resultSet.getString(5));
		    	enrichedTransaction.setLatitude(resultSet.getString(6));
		    	enrichedTransaction.setLongitude(resultSet.getString(7));
		    	enrichedTransaction.setBrand(resultSet.getString(8));
		    }
		    
		    enrichedTransaction.setProducts(products);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		if(result.getValue(Bytes.toBytes("AccountDetails"),Bytes.toBytes("accountNumber")) !=null){
			
			collector.emit(tuple, new Values((EnrichedTransaction)enrichedTransaction, stormProvenance));
			collector.ack(tuple);
		}
		else{
			System.out.println("The transaction refers to an account that is not in the data store.");
			System.out.println("Account: " + incomingTransaction.getAccountNumber());
			collector.ack(tuple);
		}
	}

	@SuppressWarnings("deprecation")
	public void prepare(Map arg0, TopologyContext context, OutputCollector collector) {
		this.constants = new Constants();
		this.componentId = context.getThisComponentId();
		this.componentType = "BOLT";
		
		System.out.println("********************** Zookeeper Host: " + constants.getZkHost());
        System.out.println("********************** Zookeeper Port: " + constants.getZkPort());
        System.out.println("********************** Zookeeper ConnString: " + constants.getZkConnString());
        System.out.println("********************** Zookeeper Kafka Path: " + constants.getZkKafkaPath());
        System.out.println("********************** Zookeeper HBase Path: " + constants.getZkHBasePath());
        System.out.println("********************** Cometd URI: " + constants.getPubSubUrl());
		
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", constants.getZkHost());
		config.set("hbase.zookeeper.property.clientPort", constants.getZkPort());
		config.set("zookeeper.znode.parent", constants.getZkHBasePath());
		
		try {
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
			conn = DriverManager.getConnection("jdbc:phoenix:"+ constants.getZkHost() + ":" + constants.getZkPort() + ":" + constants.getZkHBasePath());
			HBaseAdmin hbaseAdmin = new HBaseAdmin(config);
			
			 // Instantiating HTable
			System.out.println("******************** EnrichTransaction prepare() Creating Phoenix Tables...");
			conn.createStatement().executeUpdate("create table if not exists \"TransactionHistory\" "
						+ "(\"transactionId\" VARCHAR PRIMARY KEY, "
						+ "\"locationId\" VARCHAR, "
						+ "\"accountNumber\" VARCHAR, "
						+ "\"accountType\" VARCHAR, "
						+ "\"amount\" DOUBLE, "
						+ "\"currency\" VARCHAR, "
						+ "\"isCardPresent\" VARCHAR, "
						+ "\"transactionTimeStamp\" BIGINT) ");
			conn.commit();

			conn.createStatement().executeUpdate("create table if not exists \"TransactionItems\" "
						+ "(\"transactionItemId\" VARCHAR PRIMARY KEY, "
						+ "\"transactionId\" VARCHAR, "
						+ "\"productId\" VARCHAR) ");
			conn.commit();

			conn.createStatement().executeUpdate("create table if not exists \"Product\" "
						+ "(\"productId\" VARCHAR PRIMARY KEY, "
						+ "\"productCategory\" VARCHAR, "
						+ "\"productSubCategory\" VARCHAR, "
						+ "\"manufacturer\" VARCHAR, "
						+ "\"productName\" VARCHAR, "
						+ "\"price\" DOUBLE) ");
			conn.commit();

			conn.createStatement().executeUpdate("create table if not exists \"Location\" "
						+ "(\"locationId\" VARCHAR PRIMARY KEY, "
						+ "\"address\" VARCHAR, "
						+ "\"city\" VARCHAR, "
						+ "\"state\" VARCHAR, "
						+ "\"zip\" VARCHAR, "
						+ "\"latitude\" VARCHAR, "
						+ "\"longitude\" VARCHAR, "
						+ "\"brand\" VARCHAR) ");
			conn.commit();
			System.out.println("******************** EnrichTransaction prepare() Phoenix Tables DDL requests commited...");
			
			while(!hbaseAdmin.tableExists("TransactionHistory") 
				  && !hbaseAdmin.tableExists("TransactionItem") 
				  && !hbaseAdmin.tableExists("Product") 
				  && !hbaseAdmin.tableExists("Location")){
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				System.out.println("******************** EnrichTransaction prepare() Waiting for Phoenix Tables to be created..."); 
			}
			System.out.println("******************** EnrichTransaction prepare() Phoenix Tables created...");
			hbaseAdmin.close();
			
			System.out.println("******************** EnrichTransaction prepare() Populating Product and Location Tables...");
			String seedProducts = "UPSERT INTO \"Product\" VALUES('11','Electronics','TV','Samsung','X101','2000.00'); \n" +
					"UPSERT INTO \"Product\" VALUES('12','Electronics','DVD-Player','LG','J202','500.00'); \n" +
					"UPSERT INTO \"Product\" VALUES('13','Electronics','Sound System','Sony','C303','1000.00'); \n" +
					"UPSERT INTO \"Product\" VALUES('21','Movie','Action','NA','Gladiator', '20.00'); \n" +
					"UPSERT INTO \"Product\" VALUES('22','Movie','Comedy','NA','Wedding Crashers','22.00'); \n" +
					"UPSERT INTO \"Product\" VALUES('23','Movie','Drama','NA','Peeky Blinders','23.00'); \n" +
					"UPSERT INTO \"Product\" VALUES('31','Game','Software','Sony','God of War X','50.00'); \n" +
					"UPSERT INTO \"Product\" VALUES('32','Game','Console','Sony','PlayStation 4','200.00'); \n" +
					"UPSERT INTO \"Product\" VALUES('33','Game','Accessory','Microsoft','XBox Controller','65.00); \n" +
					"UPSERT INTO \"Product\" VALUES('41','Music','Hip-Hop','NA','JZ','15.00'); \n" +
					"UPSERT INTO \"Product\" VALUES('42','Music','Classic Rock','NA','Guns and Roses','19.00'); \n" +
					"UPSERT INTO \"Product\" VALUES('43','Music','Country','NA','Billy Ray Cyris','14.00'); \n" +
					"UPSERT INTO \"Product\" VALUES('51','Software','Game','Activision','X2: Wolverine's Revenge','45.00'); \n" +
					"UPSERT INTO \"Product\" VALUES('52','Software','Eduction','Knowledge Adventure','PlayZone! 4th - 6th Grade - Windows','20.00'); \n" +
					"UPSERT INTO \"Product\" VALUES('53','Software','Productivity','Microsoft','Office 360','150.00'); ";
			
			System.out.println("******************** EnrichTransaction prepare() seedProduct String: \n" + seedProducts);
			conn.createStatement().executeUpdate(seedProducts);
			conn.commit();
			
			String seedLocations = "UPSERT INTO \"Location\" VALUES('1000','1234 Market St.','Philadelphia','PA','19100','39.919512','-75.005711','Rays')";
			System.out.println("******************** EnrichTransaction prepare() seedProduct String: \n" + seedLocations);
			conn.createStatement().executeUpdate(seedLocations);			
			conn.commit();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		System.out.println("******************** EnrichTransaction prepare() Phoenix Tables are ready");
		this.collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("EnrichedTransaction","ProvenanceEvent"));
	}
}