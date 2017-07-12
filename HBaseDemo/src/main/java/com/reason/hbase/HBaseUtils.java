package com.reason.hbase;

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.reason.util.RandomUtil;

public class HBaseUtils {
	
	private static Logger log = Logger.getLogger(HBaseUtils.class);
	
	private static Configuration config;
	
	private static String hbaseTableName;

	/**
	 * 创建连接对象
	 * @return
	 * @throws IOException
	 */
	public static Connection createConnection() throws IOException {
		//初始化hbase配置
		if(config == null)
			config = HBaseConfiguration.create();
		//创建HBase连接
		return ConnectionFactory.createConnection(config);
	}

	public static boolean createTable(String tableName, String ... familys) throws IOException {

		Connection connection = createConnection();
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
	
	
	
	public static void set_test(int n, long t) throws IOException {
		log.info("Start function set.");
		Connection connection = createConnection();
		Table table = connection.getTable(TableName.valueOf(hbaseTableName));
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
	

	public static void get_test(String key) throws IOException {
		log.info("Start function get.");
		Connection connection = createConnection();
		Table table = connection.getTable(TableName.valueOf(hbaseTableName));
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
	
	
	public static void getByRow(String tableName, String key) throws IOException {
		log.info("Start function get.");
		Connection connection = createConnection();
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
	
	
	public static void truncate_test() throws IOException {
		log.info("Start function truncate.");
		Connection connection = createConnection();
		Admin admin = connection.getAdmin();
		admin.disableTable(TableName.valueOf(hbaseTableName));
		admin.truncateTable(TableName.valueOf(hbaseTableName), true);
		connection.close();

	}
	
	
	public static void scan_test() throws IOException {
		log.info("Start function scan.");
		Connection connection = createConnection();
		Table table = connection.getTable(TableName.valueOf(hbaseTableName));
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
	

	public static void put_test(String row, String fanmily, String column, String value) throws IOException {
		log.info("Start function put.");
		Connection connection = createConnection();
		Table table = connection.getTable(TableName.valueOf(hbaseTableName));
		Put put = new Put(Bytes.toBytes(row));
		put.addColumn(Bytes.toBytes(fanmily), Bytes.toBytes(column), Bytes.toBytes(value));
		table.put(put);
		connection.close();
	}
	
	public static void delete_test(String row, String fanmily, String column) throws IOException {
		log.info("Start function delete.");
		Connection connection = createConnection();
		Table table = connection.getTable(TableName.valueOf(hbaseTableName));
		Delete del = new Delete(Bytes.toBytes(row));
		del.addColumn(Bytes.toBytes(fanmily), Bytes.toBytes(column));
		table.delete(del);
		connection.close();
	}
	
	
    public static void selectByFilter(List<String> arr) throws IOException{  
    	log.info("Start function filter.");
    	Connection connection = createConnection();
		Table table = connection.getTable(TableName.valueOf(hbaseTableName));
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



    public static void test(){
		try {
			Connection connection = createConnection();
			long start = System.currentTimeMillis();
			System.out.println("Get table...");

			Admin admin = connection.getAdmin();
			admin.disableTable(TableName.valueOf(hbaseTableName));
			admin.truncateTable(TableName.valueOf(hbaseTableName), true);
			admin.enableTable(TableName.valueOf(hbaseTableName));
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

	public static void showTables(){
		try {
			Connection connection = createConnection();
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



	public static void jovi_test() throws IOException {
		log.info("Start function put.");
		Connection connection = createConnection();
		Table table = connection.getTable(TableName.valueOf(hbaseTableName));
		long start = System.currentTimeMillis();

		String key = "0b68a622c01e4413a499bf817b2bb47154c3c2f1f87dd0e9a4566b4997e74f9f|";
		List<Get> gets = new ArrayList<>();
		gets.add(new Get(Bytes.toBytes(key+4667450)).addFamily(Bytes.toBytes("Vote")));

//		gets.add(new Get(Bytes.toBytes(key+4667451)).addFamily(Bytes.toBytes("Vote")));
//		gets.add(new Get(Bytes.toBytes(key+4667452)).addFamily(Bytes.toBytes("Vote")));
//		gets.add(new Get(Bytes.toBytes(key+4667457)).addFamily(Bytes.toBytes("Vote")));
//		gets.add(new Get(Bytes.toBytes(key+4667480)).addFamily(Bytes.toBytes("Vote")));
//		gets.add(new Get(Bytes.toBytes(key+4667513)).addFamily(Bytes.toBytes("Vote")));
//		gets.add(new Get(Bytes.toBytes(key+4667557)).addFamily(Bytes.toBytes("Vote")));
//
//		gets.add(new Get(Bytes.toBytes(key+4667450)).addFamily(Bytes.toBytes("Vote")));
//		gets.add(new Get(Bytes.toBytes(key+4667451)).addFamily(Bytes.toBytes("Vote")));
//		gets.add(new Get(Bytes.toBytes(key+4667452)).addFamily(Bytes.toBytes("Vote")));
//		gets.add(new Get(Bytes.toBytes(key+4667457)).addFamily(Bytes.toBytes("Vote")));
//		gets.add(new Get(Bytes.toBytes(key+4667480)).addFamily(Bytes.toBytes("Vote")));
//		gets.add(new Get(Bytes.toBytes(key+4667513)).addFamily(Bytes.toBytes("Vote")));
//		gets.add(new Get(Bytes.toBytes(key+4667557)).addFamily(Bytes.toBytes("Vote")));
//
//		gets.add(new Get(Bytes.toBytes(key+4667450)).addFamily(Bytes.toBytes("Vote")));
//		gets.add(new Get(Bytes.toBytes(key+4667451)).addFamily(Bytes.toBytes("Vote")));
//		gets.add(new Get(Bytes.toBytes(key+4667452)).addFamily(Bytes.toBytes("Vote")));
//		gets.add(new Get(Bytes.toBytes(key+4667457)).addFamily(Bytes.toBytes("Vote")));
//		gets.add(new Get(Bytes.toBytes(key+4667480)).addFamily(Bytes.toBytes("Vote")));
//		gets.add(new Get(Bytes.toBytes(key+4667513)).addFamily(Bytes.toBytes("Vote")));
//		gets.add(new Get(Bytes.toBytes(key+4667557)).addFamily(Bytes.toBytes("Vote")));
		Result[] results =  table.get(gets);
		for(Result result:results){
			System.out.println(Bytes.toString(result.getRow()));
			for (Cell cell : result.rawCells()) {
				if(Bytes.toString(CellUtil.cloneQualifier(cell)).startsWith("Voted_RV-")){
					System.out.print(Bytes.toString(CellUtil.cloneQualifier(cell)).substring(9));
					System.out.println(" : "+Bytes.toString(CellUtil.cloneValue(cell)));
				}
			}
		}
		System.out.println(" Use time "+(System.currentTimeMillis() - start));

		connection.close();
	}

	public static void main(String[] args) {

		try {
			
			config = HBaseConfiguration.create();
			//config.set("hbase.zookeeper.quorum", "172.16.31.131,172.16.31.132");
			//hbaseTableName = "ecitem:EC_ItemReviewSolr";
			//config.set("hbase.zookeeper.quorum", "10.16.238.82,10.16.238.83,10.16.238.84");
			
			config.set("hbase.zookeeper.quorum", "ssspark01,ssspark03,ssspark02");
			hbaseTableName = "ecitem:EC_ItemReviewBase";
			
			//scan_test();
			
			//createTable(hbaseTableName,"BI");
			
			//put_test("key_1", "info", "c1","v1");

			jovi_test();
			//showTables();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		

	}


}
