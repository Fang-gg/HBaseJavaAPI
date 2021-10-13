import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class Demo1Test {
    public static void main(String[] args) throws IOException {
        //创建配置，指定zookeeper集群地址
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","master,node1,node2");
        //创建连接
        Connection connection = ConnectionFactory.createConnection(conf);
        //创建Admin对象
        Admin admin = connection.getAdmin();
        //例如:创建一个test_api的表
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("test_api"));
        //创建一个列簇，我这里取名叫做cf1
        HColumnDescriptor cf1 = new HColumnDescriptor("cf1");
        //对列簇进行配置
//        cf1.setTimeToLive(100); //设置过期时间
//        cf1.setMaxVersions(3); //设置版本
        //增加列簇
        hTableDescriptor.addFamily(cf1);
        admin.createTable(hTableDescriptor);
        //关闭连接
        connection.close();
    }
}
