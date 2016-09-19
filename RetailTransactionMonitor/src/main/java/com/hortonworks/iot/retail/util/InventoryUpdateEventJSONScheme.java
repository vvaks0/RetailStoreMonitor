package com.hortonworks.iot.retail.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.storm.kafka.KeyValueScheme;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.hortonworks.iot.retail.events.IncomingTransaction;
import com.hortonworks.iot.retail.events.InventoryUpdate;

public class InventoryUpdateEventJSONScheme implements KeyValueScheme {
	private static final long serialVersionUID = 1L;

	public List<Object> deserializeKeyAndValue(ByteBuffer key, ByteBuffer value) {
		String eventKey = StringScheme.deserializeString(key);
		String eventJSONString = StringScheme.deserializeString(value);
        InventoryUpdate inventoryUpdate = null;
        ObjectMapper mapper = new ObjectMapper();
        System.out.println("******************** Recieved InventoryUpdate... \n key: " + eventKey + "\n value: " + eventJSONString);
        try {
        	inventoryUpdate = mapper.readValue(eventJSONString, InventoryUpdate.class);
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
        return new Values(eventKey, inventoryUpdate);
    }

    public Fields getOutputFields() {
        return new Fields("TransactionKey", "InventoryUpdate");
    }

	@Override
	public List<Object> deserialize(ByteBuffer arg0) {
		return null;
	}
}
