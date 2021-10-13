import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;


public class Demo2API {
    Connection connection;
    TableName table = TableName.valueOf("test_api");
    @Before
    public void init() throws IOException {
        //创建配置，指定zookeeper集群地址
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","master,node1,node2");
        //创建连接
        connection = ConnectionFactory.createConnection(conf);
    }
    //put
    @Test
    public void put() throws IOException {
        //put操作
        Table test_api = connection.getTable(table);
        Put put = new Put("001".getBytes());
        put.addColumn("cf1".getBytes(),"name".getBytes(),"fangshitao".getBytes());
        test_api.put(put);
    }
    //alter table
    @Test
    public void alter() throws IOException {
        Admin admin = connection.getAdmin();
        //获取表原有的结构
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(table);

        //获取所有列簇构成的HColumnDescriptor数组
        HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
        //遍历所有的列簇
        for (HColumnDescriptor columnFamily : columnFamilies) {
            //获取列簇的名称
            String cfName = columnFamily.getNameAsString();
            //对名字名为cf1的列簇进行修改
            if("cf1".equals(cfName)){
                //修改TTL,重新设为10000
                columnFamily.setTimeToLive(10000);
            }
        }
        //修改表的结构
        admin.modifyTable(table,tableDescriptor);
    }
    //get
    @Test
    public void get() throws IOException {
        Table test_api = connection.getTable(table);
        Get get = new Get("001".getBytes());
        Result result = test_api.get(get);
        byte[] value = result.getValue("cf1".getBytes(), "name".getBytes());
        System.out.println(Bytes.toString(value));
    }
    @After
    public void close() throws IOException {
        connection.close();
    }
}
