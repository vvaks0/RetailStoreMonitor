package com.hortonworks.iot.retail.events;

public class Product {
	private String productId;
	private String productCategory;
	private String productSubCategory;
	private String manufacturer;
	private String productName;
	private Double price;
	
	public Product(){}
	
	public Product(String productId, String productCategory, String productSubCategory,String manufacturer,String productName, Double price){
		this.productId = productId;
		this.productCategory = productCategory;
		this.productSubCategory = productSubCategory;
		this.manufacturer = manufacturer;
		this.productName = productName;
		this.price = price;
	}

	public String getProductId() {
		return productId;
	}

	public void setProductId(String productId) {
		this.productId = productId;
	}

	public String getProductCategory() {
		return productCategory;
	}

	public void setProductCategory(String productCategory) {
		this.productCategory = productCategory;
	}

	public String getProductSubCategory() {
		return productSubCategory;
	}

	public void setProductSubCategory(String productSubCategory) {
		this.productSubCategory = productSubCategory;
	}

	public String getManufacturer() {
		return manufacturer;
	}

	public void setManufacturer(String manufacturer) {
		this.manufacturer = manufacturer;
	}

	public String getProductName() {
		return productName;
	}

	public void setProductName(String productName) {
		this.productName = productName;
	}

	public Double getPrice() {
		return price;
	}

	public void setPrice(Double price) {
		this.price = price;
	}
}