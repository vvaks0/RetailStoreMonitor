//transDF.write.format("orc").save("retail_transaction_history")
//transDF.write.format("orc").partition("accountType","shipToState"),save("retail_transaction_history")
//transDF.write.format("orc").partition("accountType","shipToState"),insertInto("select transactionId,locationId, item, accountNumber,amount,currency,isCardPresent,transactionTimeStamp,accountType,shipToState from phoenix_retail_transactions")

import org.apache.phoenix.spark._

val transDF = sqlContext.load( "org.apache.phoenix.spark", Map("table" -> "\"TransactionHistory\"", "zkUrl" -> "retail-demo-3-153-1-0:2181:/hbase-unsecure"))

val itemTransDF = sqlContext.load( "org.apache.phoenix.spark", Map("table" -> "\"TransactionItems\"", "zkUrl" -> "retail-demo-3-153-1-0:2181:/hbase-unsecure"))

val productDF = sqlContext.load( "org.apache.phoenix.spark", Map("table" -> "\"Product\"", "zkUrl" -> "retail-demo-3-153-1-0:2181:/hbase-unsecure"))

transDF.registerTempTable("phoenix_retail_transactions")
itemTransDF.registerTempTable("phoenix_retail_item_transactions")
productDF.registerTempTable("phoenix_retail_products")

sqlContext.setConf("hive.exec.dynamic.partition", "true")
sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

sqlContext.sql("CREATE TABLE IF NOT EXISTS retail_transaction_history (transactionId String,locationId String, item String, accountNumber String, amount Double, currency String, isCardPresent String, ipAddress String, transactionTimeStamp String) PARTITIONED BY (accountType String, shipToState String) CLUSTERED BY (accountNumber) INTO 30 BUCKETS STORED AS ORC")

sqlContext.sql("CREATE TABLE IF NOT EXISTS retail_products (productId String, productCategory String, manufacturer String, productName String, price Double) PARTITIONED BY (productSubCategory String) CLUSTERED BY (manufacturer) INTO 30 BUCKETS STORED AS ORC")

sqlContext.sql("insert overwrite table retail_transaction_history partition(accountType,shipToState) select a.transactionId, locationId, productId as item, accountNumber,amount,currency,isCardPresent,transactionTimeStamp,accountType,shipToState from phoenix_retail_transactions as a, phoenix_retail_item_transactions as b where a.transactionId = b.transactionId")

sqlContext.sql("insert overwrite table retail_products partition(productSubCategory) select productId, productCategory,  manufacturer, productName, price, productSubCategory from phoenix_retail_products")