package com.hortonworks.iot.retail.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;


import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.hortonworks.iot.retail.events.IncomingTransaction;

/*
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.kafka.KeyValueScheme;
*/

import org.apache.storm.kafka.KeyValueScheme;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class TransactionEventJSONScheme implements KeyValueScheme {
		private static final long serialVersionUID = 1L;

		public List<Object> deserializeKeyAndValue(ByteBuffer key, ByteBuffer value) {
			String eventKey = StringScheme.deserializeString(key);
			String eventJSONString = StringScheme.deserializeString(value);
	        IncomingTransaction incomingTransaction = null;
	        ObjectMapper mapper = new ObjectMapper();
	        System.out.println("******************** Recieved IncomingTransaction... \n key: " + eventKey + "\n value: " + eventJSONString);
	        try {
				incomingTransaction = mapper.readValue(eventJSONString, IncomingTransaction.class);
			} catch (JsonParseException e) {
				e.printStackTrace();
			} catch (JsonMappingException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
	        return new Values(eventKey, incomingTransaction);
	    }

	    public Fields getOutputFields() {
	        return new Fields("TransactionKey", "IncomingTransaction");
	    }

		@Override
		public List<Object> deserialize(ByteBuffer arg0) {
			return null;
		}
}