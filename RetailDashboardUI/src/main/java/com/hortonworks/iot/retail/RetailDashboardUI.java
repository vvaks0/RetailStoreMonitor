package com.hortonworks.iot.retail;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.cometd.client.BayeuxClient;
import org.cometd.client.transport.ClientTransport;
import org.cometd.client.transport.LongPollingTransport;
import org.eclipse.jetty.client.HttpClient;

@WebServlet(name = "RetailDashboardUI", urlPatterns = { "/RetailDashboard" })
public class RetailDashboardUI extends HttpServlet{
	private static final long serialVersionUID = 1L;
		private static final String CONTENT_TYPE = "text/html; charset=windows-1252";
	    private String requestType;
	    private Connection conn;
	    private String zkHost = "sandbox.hortonworks.com";
	    private String zkPort = "2181";
	    private String zkHBasePath = "/hbase-unsecure";
	    private String httpHost = "sandbox.hortonworks.com";
	    private String httpListenPort = "8082";
	    private String httpListenUri = "/contentListener";
	    private String cometdHost = "sandbox.hortonworks.com";
	    private String cometdListenPort = "8091";
		private String mapAPIKey = "NO_API_KEY_FOUND";
	    
		public void init(ServletConfig config) throws ServletException {
	    	//Configuration hbaseConfig = HBaseConfiguration.create();
	    	
	    	super.init(config);
	        System.out.println("Calling Init method and setting request to Initial");
	        requestType = "initial";
	        //testPubSub();
	        
	        Map<String, String> env = System.getenv();
	        System.out.println("********************** ENV: " + env);
	        if(env.get("ZK_HOST") != null){
	        	this.zkHost = (String)env.get("ZK_HOST");
	        }
	        if(env.get("ZK_PORT") != null){
	        	this.zkPort = (String)env.get("ZK_PORT");
	        }
	        if(env.get("ZK_HBASE_PATH") != null){
	        	this.zkHBasePath = (String)env.get("ZK_HBASE_PATH");
	        }
	        if(env.get("COMETD_HOST") != null){
	        	this.cometdHost = (String)env.get("COMETD_HOST");
	        }
	        if(env.get("COMETD_PORT") != null){
	        	this.cometdListenPort = (String)env.get("COMETD_PORT");
	        }
	        if(env.get("HTTP_HOST") != null){
	        	this.httpHost = (String)env.get("HTTP_HOST");
	        }
	        if(env.get("HTTP_PORT") != null){
	        	this.httpListenPort = (String)env.get("HTTP_PORT");
	        }
	        if(env.get("HTTP_URI") != null){
	        	this.httpListenUri = (String)env.get("HTTP_URI");
	        }
	        if(env.get("MAP_API_KEY") != null){
	        	this.mapAPIKey  = (String)env.get("MAP_API_KEY");
	        }
	        System.out.println("********************** Zookeeper Host: " + zkHost);
	        System.out.println("********************** Zookeeper: " + zkPort);
	        System.out.println("********************** Zookeeper Path: " + zkHBasePath);
	        System.out.println("********************** Cometd Host: " + cometdHost);
	        System.out.println("********************** Cometd Port: " + cometdListenPort);
	        System.out.println("********************** Http Host: " + httpHost);
	        System.out.println("********************** Http Port: " + httpListenPort);
	        System.out.println("********************** Http Uri: " + httpListenUri);
	        System.out.println("********************** Map Api Key: " + mapAPIKey);
	        
	    	//hbaseConfig.set("hbase.zookeeper.quorum", zkHost);
			//hbaseConfig.set("hbase.zookeeper.property.clientPort", zkPort);
			//hbaseConfig.set("zookeeper.znode.parent", zkHBasePath);
			
	    	try {
				Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
				conn =  DriverManager.getConnection("jdbc:phoenix:" + zkHost + ":" + zkPort + ":" + zkHBasePath);
				System.out.println("got connection to Phoenix");
	    	} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			}
	    }
	    public void doTask(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {	        
	    	response.setContentType(CONTENT_TYPE);
	        System.out.println("First Check of Request Type: " + requestType);
	        
	        if(request.getParameter("requestType") != null){
	            System.out.println("RequestType parameter : " + request.getParameter("requestType"));
	            requestType = request.getParameter("requestType");
	            System.out.println("RequestType set to :" + requestType);
	        }else{
	        	System.out.println("RequestType set to null, setting to initial");
	        	requestType = "initial";	            
	        	System.out.println("RequestType parameter : " + request.getParameter("requestType"));
	            System.out.println("RequestType set to :" + requestType);
	        }
	        
	        System.out.println("Checking if Initial: " + requestType);
	        if(requestType.equalsIgnoreCase("initial") || requestType.equalsIgnoreCase("retailDashboard")){   
	        	
	        	request.setAttribute("cometdHost", cometdHost);
	        	request.setAttribute("cometdPort", cometdListenPort);
	        	request.setAttribute("mapAPIKey", mapAPIKey);
	        	request.setAttribute("revenueByCategory", getRevenueByCategory(""));
	        	request.setAttribute("revenueBySubCategory", getRevenueBySubCategory(""));
	        	request.setAttribute("revenueByCategoryDrillDown", getRevenueByCategoryDrillDown(""));
	        	request.getRequestDispatcher("RetailDashboard.jsp").forward(request, response);
	        } 
	    }
	    public Map<String, Integer> getRevenueBySubCategory(String transactionId) {
	    	Map<String, Integer> revenueBySubCategory = new HashMap<String, Integer>();
	    	
	    	String query = "SELECT C.\"productSubCategory\", "
	    	    	+ "SUM(C.\"price\") as revenue "
	    	    	+ "FROM \"TransactionHistory\" AS A "
	    	    	+ "INNER JOIN \"TransactionItems\" AS B ON A.\"transactionId\" = B.\"transactionId\" "
	    	    	+ "INNER JOIN \"Product\" AS C ON B.\"productId\" = C.\"productId\" "
	    	    	+ "GROUP BY C.\"productSubCategory\"";
	    	
	    	ResultSet rst;
			try {
				rst = conn.createStatement().executeQuery(query);
				while (rst.next()) {
					revenueBySubCategory.put(rst.getString(1), rst.getInt(2));
		        	System.out.println(rst.getString(1) + " " + rst.getString(2));
		        }
			} catch (SQLException e) {
				e.printStackTrace();
			}
	    	
	    	return revenueBySubCategory;
		}
	    
	    public Map<String, List<ProductClassification>> getRevenueByCategoryDrillDown(String transactionId) {
	    	Map<String, List<ProductClassification>> revenueByCategory = new HashMap<String, List<ProductClassification>>();
	    	
	    	String query = "SELECT \"productCategory\", \"productSubCategory\", "
	    	    	+ "SUM(C.\"price\") AS \"revenue\" "
	    	    	+ "FROM \"TransactionHistory\" AS A "
	    	    	+ "INNER JOIN \"TransactionItems\" AS B ON A.\"transactionId\" = B.\"transactionId\" "
	    	    	+ "INNER JOIN \"Product\" AS C ON B.\"productId\" = C.\"productId\" "
	    	    	+ "GROUP BY C.\"productCategory\", C.\"productSubCategory\"";
	    	
	    	ResultSet rst;
			try {
				rst = conn.createStatement().executeQuery(query);
				String currentProductFamily;
				ProductClassification currentSubCategory;
				List<ProductClassification> subCategoryList;
				while (rst.next()) {
					currentProductFamily = rst.getString("productCategory");
					subCategoryList = new ArrayList<ProductClassification>();
					System.out.println(currentProductFamily);
					while(currentProductFamily.equalsIgnoreCase(rst.getString("productCategory")) && !rst.isClosed()){
						System.out.println(rst.getString("productSubCategory") + " " + rst.getDouble("revenue"));
						currentSubCategory = new ProductClassification(rst.getString("productCategory"), 
																	rst.getString("productSubCategory"),  
																	rst.getDouble("revenue"));
						subCategoryList.add(currentSubCategory);
						rst.next();
					}
					if(!rst.isClosed()){	
						revenueByCategory.put(currentProductFamily, subCategoryList);
					}
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
	    	
	    	return revenueByCategory;
		}
	    
	    public Map<String, Integer> getRevenueByCategory(String transactionId) {
	    	Map<String, Integer> revenueByCategory = new HashMap<String, Integer>();
	    	
	    	String query = "SELECT C.\"productCategory\", "
	    	+ "SUM(C.\"price\") as revenue "
	    	+ "FROM \"TransactionHistory\" AS A "
	    	+ "INNER JOIN \"TransactionItems\" AS B ON A.\"transactionId\" = B.\"transactionId\" "
	    	+ "INNER JOIN \"Product\" AS C ON B.\"productId\" = C.\"productId\" "
	    	+ "GROUP BY C.\"productCategory\"";
	    	
			ResultSet rst;
			try {
				rst = conn.createStatement().executeQuery(query);
				while (rst.next()) {
					revenueByCategory.put(rst.getString(1), rst.getInt(2));
		        	System.out.println(rst.getString(1) + " " + rst.getString(2));
		        }
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
			return revenueByCategory;
		}
	    
		public Map<String, Integer> getRevenueByLocation(){
			Map<String, Integer> revenueByLocation = new HashMap<String, Integer>();
			
			String query = "SELECT  \"address\", SUM(\"amount\") AS \"revenue\" "
			+ "FROM \"TransactionHistory\" AS A "
			+ "INNER JOIN \"Location\" AS B ON A.\"locationId\" = B.\"locationId\" "
			+ "GROUP BY \"address\"";
			
			ResultSet rst;
			try {
				rst = conn.createStatement().executeQuery(query);
				while (rst.next()) {
					revenueByLocation.put(rst.getString(1), rst.getInt(2));
		        	System.out.println(rst.getString(1) + " " + rst.getString(2));
		        }
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
			return revenueByLocation;
	    }
	    
	    public void testPubSub() {
	    	String pubSubUrl = "http://" + cometdHost + ":" + cometdListenPort + "/cometd";
	    	String fraudAlertChannel = "/fraudAlert";
	    	HttpClient httpClient = new HttpClient();
			try {
				httpClient.start();
			} catch (Exception e) {
				e.printStackTrace();
			}

			// Prepare the transport
			Map<String, Object> options = new HashMap<String, Object>();
			ClientTransport transport = new LongPollingTransport(options, httpClient);

			// Create the BayeuxClient
			BayeuxClient bayuexClient = new BayeuxClient(pubSubUrl, transport);
			
			bayuexClient.handshake();
			boolean handshaken = bayuexClient.waitFor(5000, BayeuxClient.State.CONNECTED);
			if (handshaken)
			{
				System.out.println("Connected to Cometd Http PubSub Platform");
			}
			else{
				System.out.println("Could not connect to Cometd Http PubSub Platform");
			}
			
			bayuexClient.getChannel(fraudAlertChannel).publish("TEST");
	    }
	    
	    public String convertPOJOToJSON(Object pojo) {
	        String jsonString = "";
	        ObjectMapper mapper = new ObjectMapper();

	        try {
	            jsonString = mapper.writeValueAsString(pojo);
	        } catch (JsonGenerationException e) {
	            e.printStackTrace();
	        } catch (JsonMappingException e) {
	            e.printStackTrace();
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	        return jsonString;
	    }
	    
		public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
	        this.doTask(request, response);
	    }
	    public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
	       this.doTask(request, response);
	    }
}