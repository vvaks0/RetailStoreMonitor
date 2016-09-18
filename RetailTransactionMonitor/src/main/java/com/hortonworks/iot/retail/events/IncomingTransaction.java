package com.hortonworks.iot.retail.events;

import java.util.List;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

@JsonIgnoreProperties(value = { "state","status" })
public class IncomingTransaction {
	private String transactionId;
	private String locationId;
	private List<String> items;
	private String accountNumber;
	private String accountType;
	private Double amount;
	private String currency;
	private String isCardPresent;
	private String ipAddress;
	private String transactionTimeStamp;
	
	public String getAccountNumber(){
		return accountNumber;
	}
	public String getAccountType() {
		return accountType;
	}
	public String getTransactionId(){
		return transactionId;
	}
	public Double getAmount(){
		return amount;
	}
	public String getCurrency(){
		return currency;
	}
	public String getIsCardPresent(){
		return isCardPresent;
	}
	public void setAccountNumber(String value){
		accountNumber = value;
	}
	public void setAccountType(String accountType) {
		this.accountType = accountType;
	}
	public void setTransactionId(String value){
		transactionId = value;
	}
	public void setAmount(Double value){
		amount = value;
	}
	public void setCurrency(String value){
		currency = value;
	}
	public void setIsCardPresent(String value){
		isCardPresent = value;
	}
	public String getTransactionTimeStamp() {
		return transactionTimeStamp;
	}
	public void setTransactionTimeStamp(String transactionTimeStamp) {
		this.transactionTimeStamp = transactionTimeStamp;
	}
	public String getIpAddress() {
		return ipAddress;
	}
	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}
	public String getLocationId() {
		return locationId;
	}
	@JsonProperty("serialNumber")
	public void setLocationId(String locationId) {
		this.locationId = locationId;
	}
	public List<String> getItems() {
		return items;
	}
	public void setItems(List<String> items) {
		this.items = items;
	}
}
