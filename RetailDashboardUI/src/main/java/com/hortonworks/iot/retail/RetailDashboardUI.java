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
	    private HTable customerAccountTable = null;
	    private HTable transactionHistoryTable = null;
	    private String zkHost = "sandbox.hortonworks.com";
	    private String zkPort = "2181";
	    private String zkHBasePath = "/hbase-unsecure";
	    private String httpHost = "sandbox.hortonworks.com";
	    private String httpListenPort = "8082";
	    private String httpListenUri = "/contentListener";
	    private String cometdHost = "sandbox.hortonworks.com";
	    private String cometdListenPort = "8091";
		private String defaultAccountNumber = "19123";
		private String mapAPIKey = "NO_API_KEY_FOUND";
	    
	    @SuppressWarnings("deprecation")
		public void init(ServletConfig config) throws ServletException {
	    	Configuration hbaseConfig = HBaseConfiguration.create();
			
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
	        
	    	hbaseConfig.set("hbase.zookeeper.quorum", zkHost);
			hbaseConfig.set("hbase.zookeeper.property.clientPort", zkPort);
			hbaseConfig.set("zookeeper.znode.parent", zkHBasePath);
			
	    }
	    public void doTask(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
	    	String accountNumber;
	    	AccountDetails accountDetails = null;
	    	String fraudulentTransactionId = null;
	    	Transaction fraudulentTransaction = null;
	    	List<AccountDetails> accountDetailsList = null;
	    	List<Transaction> transactionHistory = null;
	    	Map<String, Integer> merchantTypeShare = null;
	        
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
	        	request.setAttribute("accountDetails", accountDetails);
	        	request.setAttribute("transactionHistory", transactionHistory);
	        	request.getRequestDispatcher("RetailDashboard.jsp").forward(request, response);
	        } 
	    }
	    public Transaction getRevenueBySubCategory(String transactionId) {
	    	String query = "SELECT A.\"locationId\", "
	    	+ "C.\"productCategory\", "
	    	+ "C.\"productSubCategory\", "
	    	+ "SUM(C.\"price\") as revenue "
	    	+ "FROM \"TransactionHistory\" AS A "
	    	+ "INNER JOIN \"TransactionItems\" AS B ON A.\"transactionId\" = B.\"transactionId\" "
	    	+ "INNER JOIN \"Product\" AS C ON B.\"productId\" = C.\"productId\" "
	    	+ "GROUP BY A.\"locationId\", C.\"productCategory\", C.\"productSubCategory\"";
	    	
	    	return null;
		}
	    
	    public Transaction getRevenueByCategory(String transactionId) {
	    	String query = "SELECT A.\"locationId\", "
	    	+ "C.\"productCategory\", "
	    	+ "SUM(C.\"price\") as revenue "
	    	+ "FROM \"TransactionHistory\" AS A "
	    	+ "INNER JOIN \"TransactionItems\" AS B ON A.\"transactionId\" = B.\"transactionId\" "
	    	+ "INNER JOIN \"Product\" AS C ON B.\"productId\" = C.\"productId\" "
	    	+ "GROUP BY A.\"locationId\", C.\"productCategory\"";
	    	
	    	return null;
		}
	    
		public List<AccountDetails> getRevenueByLocation(){
			String query = "SELECT  \"address\", SUM(\"amount\") AS \"revenue\" "
			+ "FROM \"TransactionHistory\" AS A "
			+ "INNER JOIN \"Location\" AS B ON A.\"locationId\" = B.\"locationId\" "
			+ "GROUP BY \"address\"";
			
			return null;
	    }
	    
	    public AccountDetails getInventoryLevelsByLocation(String accountNumber){
	    	return null;
	    }
	    
	    public Map<String, Integer> getMerchantTypeShare() throws ClassNotFoundException, SQLException{
	    	Map<String, Integer> merchantTypeShare = new HashMap<String, Integer>();
	    	Connection conn;
	        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
	        conn =  DriverManager.getConnection("jdbc:phoenix:" + zkHost + ":" + zkPort + ":" + zkHBasePath);
	        System.out.println("got connection");
	        ResultSet rst = conn.createStatement().executeQuery("SELECT \"merchantType\", COUNT(\"merchantType\") as \"Count\" FROM \"TransactionHistory\" WHERE \"frauduent\" = 'false' GROUP BY \"merchantType\"");
	        while (rst.next()) {
	        	merchantTypeShare.put(rst.getString(1), rst.getInt(2));
	        	System.out.println(rst.getString(1) + " " + rst.getString(2));
	        }
	        
	        return merchantTypeShare;
	    }
	    
	    public void sendFraudNotification(Transaction transaction){
	        System.out.println("Sending Customer Notification Information ************************");
	        transaction.setSource("analyst_action");
	        transaction.setFraudulent("true");
	        try{
	        	URL url = new URL("http://" + httpHost + ":" + httpListenPort + httpListenUri);
	    		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
	    		conn.setDoOutput(true);
	    		conn.setRequestMethod("POST");
	    		conn.setRequestProperty("Content-Type", "application/json");
	            String payload = "{\"to\": \"/topics/fraudAlert\",\"data\": " + convertPOJOToJSON(transaction) + "}"; 
	    		System.out.println("To String: " + payload);
	            
	            OutputStream os = conn.getOutputStream();
	    		os.write(payload.getBytes());
	    		os.flush();
	            
	            if (conn.getResponseCode() != 200)
	    			throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
	    		
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
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