package com.hortonworks.iot.retail.topology;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.KeyValueSchemeAsMultiScheme;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.shade.org.joda.time.Duration;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

import static org.apache.storm.topology.base.BaseWindowedBolt.Count;

/*
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy; */

import com.hortonworks.iot.retail.bolts.AtlasLineageReporter;
import com.hortonworks.iot.retail.bolts.EnrichInventoryUpdate;
import com.hortonworks.iot.retail.bolts.EnrichTransaction;
import com.hortonworks.iot.retail.bolts.TransactionMonitor;
import com.hortonworks.iot.retail.bolts.InstantiateProvenance;
import com.hortonworks.iot.retail.bolts.MergeStreams;
import com.hortonworks.iot.retail.bolts.ProcessSocialMediaEvent;
import com.hortonworks.iot.retail.bolts.PublishInventoryUpdate;
import com.hortonworks.iot.retail.bolts.PublishSocialSentiment;
import com.hortonworks.iot.retail.bolts.PublishTransaction;
import com.hortonworks.iot.retail.util.Constants;
import com.hortonworks.iot.retail.util.InventoryUpdateEventJSONScheme;
import com.hortonworks.iot.retail.util.SocialMediaEventJSONScheme;
import com.hortonworks.iot.retail.util.TransactionEventJSONScheme;

/*
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.KeyValueSchemeAsMultiScheme;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
*/

public class RetailTransactionMonitorTopology {
	 public static void main(String[] args) {
	     TopologyBuilder builder = new TopologyBuilder();
	     Constants constants = new Constants();   
	  	 /*
	     RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter(",");
	  	 SyncPolicy syncPolicy = new CountSyncPolicy(1000);
	  	 FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);

	  	 FileNameFormat transactionLogFileNameFormat = new DefaultFileNameFormat().withPath(constants.getHivePath());
	  	 HdfsBolt LogTransactionHdfsBolt = new HdfsBolt()
	  		     .withFsUrl(constants.getNameNode())
	  		     .withFileNameFormat(transactionLogFileNameFormat)
	  		     .withRecordFormat(format)
	  		     .withRotationPolicy(rotationPolicy)
	  		     .withSyncPolicy(syncPolicy); */
	  	
	  	System.out.println("********************** Starting Topology.......");
	  	System.out.println("********************** Zookeeper Host: " + constants.getZkHost());
        System.out.println("********************** Zookeeper Port: " + constants.getZkPort());
        System.out.println("********************** Zookeeper ConnString: " + constants.getZkConnString());
        System.out.println("********************** Zookeeper Kafka Path: " + constants.getZkKafkaPath());
        System.out.println("********************** Zookeeper HBase Path: " + constants.getZkHBasePath());
        System.out.println("********************** Atlas Host: " + constants.getAtlasHost());
        System.out.println("********************** Atlas Port: " + constants.getAtlasPort());
        System.out.println("********************** Cometd URI: " + constants.getPubSubUrl());
	  	  
	      Config conf = new Config(); 
	      BrokerHosts hosts = new ZkHosts(constants.getZkConnString(), constants.getZkKafkaPath());
	      
	      SpoutConfig incomingTransactionsKafkaSpoutConfig = new SpoutConfig(hosts, constants.getIncomingTransactionsTopicName(), constants.getZkKafkaPath(), UUID.randomUUID().toString());
	      incomingTransactionsKafkaSpoutConfig.scheme = new KeyValueSchemeAsMultiScheme(new TransactionEventJSONScheme());
	      incomingTransactionsKafkaSpoutConfig.ignoreZkOffsets = true;
	      incomingTransactionsKafkaSpoutConfig.useStartOffsetTimeIfOffsetOutOfRange = true;
	      incomingTransactionsKafkaSpoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
	      KafkaSpout incomingTransactionsKafkaSpout = new KafkaSpout(incomingTransactionsKafkaSpoutConfig); 
	      
	      SpoutConfig inventoryUpdatesKafkaSpoutConfig = new SpoutConfig(hosts, constants.getInventoryUpdatesTopicName(), constants.getZkKafkaPath(), UUID.randomUUID().toString());
	      inventoryUpdatesKafkaSpoutConfig.scheme = new KeyValueSchemeAsMultiScheme(new InventoryUpdateEventJSONScheme());
	      inventoryUpdatesKafkaSpoutConfig.ignoreZkOffsets = true;
	      inventoryUpdatesKafkaSpoutConfig.useStartOffsetTimeIfOffsetOutOfRange = true;
	      inventoryUpdatesKafkaSpoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
	      KafkaSpout inventoryUpdatesKafkaSpout = new KafkaSpout(inventoryUpdatesKafkaSpoutConfig); 
	      
	      SpoutConfig socialMediaKafkaSpoutConfig = new SpoutConfig(hosts, constants.getSocialMediaTopicName(), constants.getZkKafkaPath(), UUID.randomUUID().toString());
	      socialMediaKafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new SocialMediaEventJSONScheme());
	      socialMediaKafkaSpoutConfig.ignoreZkOffsets = true;
	      socialMediaKafkaSpoutConfig.useStartOffsetTimeIfOffsetOutOfRange = true;
	      socialMediaKafkaSpoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
	      KafkaSpout socialMediaKafkaSpout = new KafkaSpout(socialMediaKafkaSpoutConfig);
	      
	      
	      //builder.setSpout("IncomingTransactionsKafkaSpout", incomingTransactionsKafkaSpout);
	      //builder.setBolt("InstantiateProvenance", new InstantiateProvenance(), 1).shuffleGrouping("IncomingTransactionsKafkaSpout");
	      //builder.setBolt("EnrichTransaction", new EnrichTransaction(), 1).shuffleGrouping("InstantiateProvenance");
	      //builder.setBolt("PublishTransaction", new PublishTransaction(), 1).shuffleGrouping("EnrichTransaction", "TransactionStream");
	      //builder.setBolt("TransactionMonitor", new TransactionMonitor().withWindow(new Duration(10), new Duration(5)),1).shuffleGrouping("EnrichTransaction", "TransactionStream").shuffleGrouping("EnrichInventoryUpdate", "InventoryStream");
	      //builder.setBolt("AtlasLineageReporter", new AtlasLineageReporter(), 1).shuffleGrouping("TransactionMonitor", "ProvenanceRegistrationStream");
	      
	      //builder.setSpout("InventoryUpdatesKafkaSpout", inventoryUpdatesKafkaSpout);
	      //builder.setBolt("EnrichInventoryUpdate", new EnrichInventoryUpdate(), 1).shuffleGrouping("InventoryUpdatesKafkaSpout");
	      //builder.setBolt("PublishInventoryUpdate", new PublishInventoryUpdate(), 1).shuffleGrouping("EnrichInventoryUpdate", "InventoryStream");
	      //builder.setBolt("MergeStreams", new MergeStreams().withWindow(new Count(12), new Count(6)), 1).shuffleGrouping("EnrichTransaction", "TransactionStream");//.shuffleGrouping("EnrichInventoryUpdate", "InventoryStream");
	      
	      builder.setSpout("SocialMediaKafkaSpout", socialMediaKafkaSpout);
	      //builder.setBolt("ProcessSocialMediaEvent", new ProcessSocialMediaEvent(), 1).shuffleGrouping("SocialMediaKafkaSpout");
	      //builder.setBolt("PublishSocialMediaEvent", new PublishSocialSentiment(), 1).shuffleGrouping("ProcessSocialMediaEvent", "SocialMediaStream");
	      
	      conf.setNumWorkers(1);
	      conf.setMaxSpoutPending(5000);
	      conf.setMaxTaskParallelism(1);
	      
	      //submitToLocal(builder, conf);
	      submitToCluster(builder, conf);
	 }
	 
	 public static void submitToLocal(TopologyBuilder builder, Config conf){
		 LocalCluster cluster = new LocalCluster();
		 cluster.submitTopology("RetailTransactionMonitor", conf, builder.createTopology()); 
	 }
	 
	 public static void submitToCluster(TopologyBuilder builder, Config conf){
		 try {
				StormSubmitter.submitTopology("RetailTransactionMonitor", conf, builder.createTopology());
		      } catch (AlreadyAliveException e) {
				e.printStackTrace();
		      } catch (InvalidTopologyException e) {
				e.printStackTrace();
		      } catch (AuthorizationException e) {
				e.printStackTrace();
		      }
	 }
}
