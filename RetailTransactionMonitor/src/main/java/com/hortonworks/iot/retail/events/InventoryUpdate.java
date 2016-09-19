package com.hortonworks.iot.retail.events;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

@JsonIgnoreProperties(value = { "state","status" })
public class InventoryUpdate {
	private String productId;
	private String locationId;
	
	public String getLocationId() {
		return locationId;
	}
	@JsonProperty("serialNumber")
	public void setLocationId(String locationId) {
		this.locationId = locationId;
	}
	public String getProductId() {
		return productId;
	}
	public void setProductId(String productId) {
		this.productId = productId;
	}
}
