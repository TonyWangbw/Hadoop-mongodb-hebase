import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
 
import java.io.IOException;
public class ExampleForHBase {
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;
    public static void main(String[] args)throws IOException{
        init();
//        创建表
        createTable("student",new String[]{"score"});
//        添加数据
        insertData("student","syj","score","Math","120");
        insertData("student","syj","score","CS","120");
        insertData("student","scs","score","Math","125");
        insertData("student","scs","score","OS","81");
//        打印表
        printTable("student");
//        查看数据
        getData("student", "syj", "score","Math");
        getData("student", "scs", "score","Math");
//        删除数据
        deleteData("student","scs", "score","Math","OS");
        
        printTable("student");
//        删除表
        deleteTable("student");
        close();
    }
 
    public static void init(){
        configuration  = HBaseConfiguration.create();
        configuration.set("hbase.rootdir","hdfs://localhost:9000/hbase");
        try{
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        }catch (IOException e){
            e.printStackTrace();
        }
    }
 
    public static void close(){
        try{
            if(admin != null){
                admin.close();
            }
            if(null != connection){
                connection.close();
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    public static void deleteTable(String tableName) throws IOException {
        init();
        TableName tn = TableName.valueOf(tableName);
        if (admin.tableExists(tn)) {
            admin.disableTable(tn);
            admin.deleteTable(tn);
        }
        System.out.println("删除成功");
        
        close();
    }
    public static void createTable(String myTableName,String[] colFamily) throws IOException {
        TableName tableName = TableName.valueOf(myTableName);
        if(admin.tableExists(tableName)){
            System.out.println("talbe is exists!");
        }else {
            TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(tableName);
            for(String str:colFamily){
                ColumnFamilyDescriptor family = 
ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(str)).build();
                tableDescriptor.setColumnFamily(family);
            }
            admin.createTable(tableDescriptor.build());
        } 
        System.out.println("创建成功");
        
    }
 
    public static void insertData(String tableName,String rowKey,String colFamily,String col,String val) throws IOException { 
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(colFamily.getBytes(),col.getBytes(), val.getBytes());
        table.put(put);
        table.close(); 
        System.out.println("添加成功");
    }
    public static void deleteData(String tableName,String rowKey,String colFamily, String col,String col2)throws  IOException{ 
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete =new Delete(rowKey.getBytes());
        table.delete(delete);
        table.close(); 
        System.out.println("删除成功");
    }
    public static void getData(String tableName,String rowKey,String colFamily, String col)throws  IOException{ 
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        get.addColumn(colFamily.getBytes(),col.getBytes());
        Result result = table.get(get);
        System.out.println("查找成功：");
        System.out.println(new String(result.getValue(colFamily.getBytes(),col==null?null:col.getBytes())));
        table.close(); 
    }


    public static void printTable(String tableName) throws IOException {
	 Table table = connection.getTable(TableName.valueOf(tableName));
	    System.out.println("表格：");
	    Scan scan = new Scan();
	    ResultScanner scanner = table.getScanner(scan);
	    for (Result result : scanner) {
	        byte[] rowKey = result.getRow();
	        System.out.print("row key: " + Bytes.toString(rowKey) + "\t");
	        Cell[] cells = result.rawCells();
	        for (Cell cell : cells) {
	            byte[] family = CellUtil.cloneFamily(cell);
	            byte[] qualifier = CellUtil.cloneQualifier(cell);
	            byte[] value = CellUtil.cloneValue(cell);
	            System.out.print(Bytes.toString(family) + ":" + Bytes.toString(qualifier) + "=" + Bytes.toString(value) + "\t");
	        }
	        System.out.println();
	    }
	    scanner.close();
	    table.close();

 		}}