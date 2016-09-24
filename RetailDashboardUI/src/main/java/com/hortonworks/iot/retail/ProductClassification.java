package com.hortonworks.iot.retail;

public class ProductClassification {
	private String productCategory;
	private String productSubCategory;
	private Double amount;
	
	public ProductClassification(){}
	
	public ProductClassification(String productCategory, String productSubCategory, Double amount){
		this.productCategory = productCategory;
		this.productSubCategory = productSubCategory;
		this.amount = amount;
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

	public Double getPrice() {
		return amount;
	}

	public void setPrice(Double amount) {
		this.amount = amount;
	}
}