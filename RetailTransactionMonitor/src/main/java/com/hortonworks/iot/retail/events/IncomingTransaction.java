package com.hortonworks.iot.retail.events;

import java.util.List;

public class IncomingTransaction {
	private String transactionId;
	private String [] items;
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
	public String[] getItems() {
		return items;
	}
	public void setItems(String[] items) {
		this.items = items;
	}
}
