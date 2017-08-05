package com.reason.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.exceptions.HBaseException;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.reason.util.RandomUtil;

public class HBaseUtils {

	private static Logger log = Logger.getLogger(HBaseUtils.class);
	private static Connection conn;
	private static Configuration config;
	
	public static void init(String hosts){
		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", hosts);
	}

	public static void refreshConnection() throws Exception{
		if(config != null){
			if(conn != null){
				try {
					conn.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			conn = ConnectionFactory.createConnection(config);
		}
	}

	/**
	 * 创建连接对象
	 * @return
	 * @throws Exception
	 */
	public static Connection getConnection() throws Exception {
		//初始化hbase配置
		if(conn == null){
			if(config == null){
				throw new HBaseException("Please init hbase in before.");
			}
			conn = ConnectionFactory.createConnection(config);
		}
		//创建HBase连接
		return conn;
	}

	public static Table getTable(String tableName) throws Exception{
		return getConnection().getTable(TableName.valueOf(tableName));
	}
	
	
	public static void closeTable(Table table){
		if(null != table){
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
				log.error("close table error",e);
			}
		}
	}


	public static void closeConnection(){
		if(conn != null){
			try {
				conn.close();
			} catch (IOException e) {
				e.printStackTrace();
				log.error("close connection error",e);
			}
		}
	}

	public static boolean createTable(String tableName, String ... familys) throws Exception {

		Connection connection = getConnection();
		Admin admin = connection.getAdmin();
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));

		for(String family:familys){
			desc.addFamily(new HColumnDescriptor(family));
		}

		admin.createTable(desc);
		admin.close();
		connection.close();
		return false;
	}
	
	
	
	public static void set_test(String tableName,int n, long t) throws Exception {
		log.info("Start function set.");
		Connection connection = getConnection();
		Table table = connection.getTable(TableName.valueOf(tableName));
		long start = System.currentTimeMillis();
		int s = 1000000;
		int e = s+n;
		for(int i=s; i<e; i++){
			String rowKey = RandomUtil.fixSize(i,10)+"_"+t;
			
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn("bs".getBytes(), "name".getBytes(), Bytes.toBytes(RandomUtil.getRandomString(3)));
			put.addColumn("bs".getBytes(), "age".getBytes(), Bytes.toBytes(RandomUtil.getAge()));
			put.addColumn("bs".getBytes(), "sex".getBytes(), Bytes.toBytes(RandomUtil.getRandomSex()));
			put.addColumn("bs".getBytes(), "height".getBytes(), Bytes.toBytes(RandomUtil.getRandomInt(80, 200)));
			
			table.put(put);
			if((i-s)%(n/10)==0){
				log.debug(rowKey+" -->"+(i-s)/(n/10)*10+"%");
			}
		}
		
		long end = System.currentTimeMillis();
		log.debug("Use times:"+((end-start)/1000));
		table.close();
		connection.close();

	}
	

	public static void get_test(String tableName, String key) throws Exception {
		log.info("Start function get.");
		Connection connection = getConnection();
		Table table = connection.getTable(TableName.valueOf(tableName));
		StopWatch time = new StopWatch();
		Get get = new Get(Bytes.toBytes(key));
		time.start();
		Result result = table.get(get);
		for (Cell cell : result.rawCells()) {
    		if("age1".equals(new String(CellUtil.cloneQualifier(cell))) || "height".equals(new String(CellUtil.cloneQualifier(cell)))){
    			System.out.println(new String(CellUtil.cloneQualifier(cell))+":"+Bytes.toInt(CellUtil.cloneValue(cell)));
    		}else{
    			System.out.println(new String(CellUtil.cloneQualifier(cell))+":"+new String(CellUtil.cloneValue(cell)));
    		}
		}
		time.stop();
		connection.close();
		log.debug("Use time " + time.getTime() + " ms");

	}
	
	
	public static void getByRow(String tableName, String key) throws Exception {
		log.info("Start function get.");
		Connection connection = getConnection();
		Table table = connection.getTable(TableName.valueOf(tableName));
		StopWatch time = new StopWatch();
		Get get = new Get(Bytes.toBytes(key));
		time.start();
		Result result = table.get(get);
		for (Cell cell : result.rawCells()) {
    		if("age1".equals(new String(CellUtil.cloneQualifier(cell))) || "height".equals(new String(CellUtil.cloneQualifier(cell)))){
    			System.out.println(new String(CellUtil.cloneQualifier(cell))+":"+Bytes.toInt(CellUtil.cloneValue(cell)));
    		}else{
    			System.out.println(new String(CellUtil.cloneQualifier(cell))+":"+new String(CellUtil.cloneValue(cell)));
    		}
		}
		time.stop();
		connection.close();
		log.debug("Use time " + time.getTime() + " ms");

	}
	
	
	public static void truncate_test(String tableName) throws Exception {
		log.info("Start function truncate.");
		Connection connection = getConnection();
		Admin admin = connection.getAdmin();
		admin.disableTable(TableName.valueOf(tableName));
		admin.truncateTable(TableName.valueOf(tableName), true);
		connection.close();

	}
	
	
	public static void scan_test(String tableName) throws Exception {
		log.info("Start function scan.");
		Connection connection = getConnection();
		Table table = connection.getTable(TableName.valueOf(tableName));
		Scan ss = new Scan();
		ss.setCaching(5);
		ResultScanner rs = table.getScanner(ss);
		for( Result result:rs){
        	for (Cell cell : result.rawCells()) {
        		if("age".equals(new String(CellUtil.cloneQualifier(cell))) || "height".equals(new String(CellUtil.cloneQualifier(cell)))){
        			System.out.println(new String(CellUtil.cloneQualifier(cell))+":"+Bytes.toInt(CellUtil.cloneValue(cell)));
        		}else{
        			System.out.println(new String(CellUtil.cloneQualifier(cell))+":"+new String(CellUtil.cloneValue(cell)));
        		}
			}
        }
		connection.close();
	}
	

	public static void put_test(String tableName, String row, String fanmily, String column, String value) throws Exception {
		log.info("Start function put.");
		Connection connection = getConnection();
		Table table = connection.getTable(TableName.valueOf(tableName));
		Put put = new Put(Bytes.toBytes(row));
		put.addColumn(Bytes.toBytes(fanmily), Bytes.toBytes(column), Bytes.toBytes(value));
		table.put(put);
		connection.close();
	}
	
	public static void delete_test(String tableName, String row, String fanmily, String column) throws Exception {
		log.info("Start function delete.");
		Connection connection = getConnection();
		Table table = connection.getTable(TableName.valueOf(tableName));
		Delete del = new Delete(Bytes.toBytes(row));
		del.addColumn(Bytes.toBytes(fanmily), Bytes.toBytes(column));
		table.delete(del);
		connection.close();
	}
	
	
    public static void selectByFilter(String tableName, List<String> arr) throws Exception{
    	log.info("Start function filter.");
    	Connection connection = getConnection();
		Table table = connection.getTable(TableName.valueOf(tableName));
		StopWatch time = new StopWatch();
		time.start();
        FilterList filterList = new FilterList();  
        Scan ss = new Scan();  
        for(String v:arr){ // 各个条件之间是“与”的关系  
            String [] s=v.split(",");  
            filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes(s[0]),  
                                                             Bytes.toBytes(s[1]),  
                                                             CompareOp.EQUAL,Bytes.toBytes(s[2])  
                                                             )  
            );  
            // 添加下面这一行后，则只返回指定的cell，同一行中的其他cell不返回  
			// s1.addColumn(Bytes.toBytes(s[0]), Bytes.toBytes(s[1]));  
        }  
        ss.setFilter(filterList);  
        ResultScanner resultList = table.getScanner(ss); 
        
        for( Result result:resultList){
        	for (Cell cell : result.rawCells()) {
        		if("name".equals(new String(CellUtil.cloneQualifier(cell))) || "sex".equals(new String(CellUtil.cloneQualifier(cell)))){
        			System.out.println(new String(CellUtil.cloneQualifier(cell))+":"+new String(CellUtil.cloneValue(cell)));
        		}else{
        			System.out.println(new String(CellUtil.cloneQualifier(cell))+":"+Bytes.toInt(CellUtil.cloneValue(cell)));
        		}
			}
        }
        time.stop();
        log.debug("Use time " + time.getTime());
        
    }  



    public static void test(String tableName) throws Exception{
		try {
			Connection connection = getConnection();
			long start = System.currentTimeMillis();
			System.out.println("Get table...");

			Admin admin = connection.getAdmin();
			admin.disableTable(TableName.valueOf(tableName));
			admin.truncateTable(TableName.valueOf(tableName), true);
			admin.enableTable(TableName.valueOf(tableName));
			TableName[] tables = admin.listTableNames();
			for (TableName t:tables){
                System.out.println(Bytes.toString(t.getName()));
            }
			System.out.println("use time " +(System.currentTimeMillis() - start)+ " ms.");
			connection.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void showTables() throws Exception{
		try {
			Connection connection = getConnection();
			long start = System.currentTimeMillis();
			System.out.println("Show table...");
			Admin admin = connection.getAdmin();
			TableName[] tables = admin.listTableNames();
			for (TableName t:tables){
				System.out.println(Bytes.toString(t.getName()));
			}
			System.out.println("use time " +(System.currentTimeMillis() - start)+ " ms.");
			connection.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}




	public static void main(String[] args) {

		try {

			HBaseUtils.init("ssspark01,ssspark03,ssspark02");
			//HBaseUtils.init("sxlab19-0,sxlab19-1,sxlab19-2");

			//createTable("reason_test", "BI");
			//Table table = getTable("reason:test");
			Table table = getTable("ecitem:EC_ItemReviewBase");


			Put putf = new Put(Bytes.toBytes("12716499d71d412e79c0a38abaf3faba144c2593f2b2a6a76ab893672a2e47f2|4621590"));
			putf.addColumn(Bytes.toBytes("BaseInfo"),Bytes.toBytes("approve"),Bytes.toBytes("Y"));

			//Delete del = new Delete(Bytes.toBytes("12716499d71d412e79c0a38abaf3faba144c2593f2b2a6a76ab893672a2e47f2|4621590"));
			//del.addColumn(Bytes.toBytes("BaseInfo"),Bytes.toBytes("Approve"));

			//table.delete(del);
			table.put(putf);

//			Put putf = new Put(Bytes.toBytes("key1"));
//			putf.addColumn(Bytes.toBytes("BI"),Bytes.toBytes("C1"),Bytes.toBytes("V7"));
//			putf.addColumn(Bytes.toBytes("info"),Bytes.toBytes("C1"),Bytes.toBytes("V4"));
//			Delete del = new Delete(Bytes.toBytes("key1"));
//
//			Put put = new Put(Bytes.toBytes("key1"));
//			put.addColumn(Bytes.toBytes("BI"),Bytes.toBytes("C1"),Bytes.toBytes("V8"));
//			table.put(putf);
//			table.delete(del);
//			table.put(put);

			HBaseUtils.closeTable(table);
			HBaseUtils.closeConnection();

			//showTables();




		} catch (Exception e) {
			e.printStackTrace();
		}
		
		

	}


}
