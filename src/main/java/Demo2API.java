import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;


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
    //putAll 读取students.txt 并且将数据写入HBase
    @Test
    public void putAll() throws IOException {
        //创建students表 info
        Admin admin = connection.getAdmin();
        TableName studentsT = TableName.valueOf("students");
        //判断表是否存在
        if(!admin.tableExists(studentsT)){
            HTableDescriptor hTableDescriptor = new HTableDescriptor(studentsT);
            HColumnDescriptor info = new HColumnDescriptor("info");
            HTableDescriptor hTableDescriptor1 = hTableDescriptor.addFamily(info);
            admin.createTable(hTableDescriptor);
        }
        Table stu = connection.getTable(studentsT);

        BufferedReader br = new BufferedReader(new FileReader("D:\\Maven\\HBase\\data\\students.txt"));
        String line = null;
        ArrayList<Put> puts = new ArrayList<>();
        int batchSize = 11;
        while ((line = br.readLine()) != null) {

            // 读取每一行数据
            String[] split = line.split(",");
            String id = split[0];
            String name = split[1];
            String age = split[2];
            String gender = split[3];
            String clazz = split[4];
            Put put = new Put(id.getBytes());
            put.addColumn("info".getBytes(), "name".getBytes(), name.getBytes());
            put.addColumn("info".getBytes(), "age".getBytes(), age.getBytes());
            put.addColumn("info".getBytes(), "gender".getBytes(), gender.getBytes());
            put.addColumn("info".getBytes(), "clazz".getBytes(), clazz.getBytes());
            puts.add(put); // 将每条数据构建好的put对象加入puts列表
            if (puts.size() == batchSize) {
                stu.put(puts); // 批量写入
                puts = new ArrayList<>();
            }

//            stu.put(put); // 逐条put

        }
        if (puts.size() != 0) {
            stu.put(puts); // 批量写入
        }

    }
    //scan
    @Test
    public void scan() throws IOException {
        Table students = connection.getTable(TableName.valueOf("students"));
        Scan scan = new Scan();
        scan.withStartRow("1500100970".getBytes());//开始设置的Row
        scan.withStopRow("1500100980".getBytes());//结束设置的Row
        ResultScanner scanner = students.getScanner(scan);
        for (Result result : scanner) {
            String id = Bytes.toString(result.getRow());
            String name = Bytes.toString(result.getValue("info".getBytes(), "name".getBytes()));
            String age = Bytes.toString(result.getValue("info".getBytes(), "age".getBytes()));
            String gender = Bytes.toString(result.getValue("info".getBytes(), "gender".getBytes()));
            String clazz = Bytes.toString(result.getValue("info".getBytes(), "clazz".getBytes()));
            System.out.println(id+","+name+","+age+","+gender+","+clazz);
        }
    }
    // 获取数据的另外一种方式
    // 适用于每条数据结构不唯一的情况下 直接遍历每条数据包含的所有的cell
    @Test
    public void scanWithUtil() throws IOException {
        Table students = connection.getTable(TableName.valueOf("students"));
        Scan scan = new Scan();
        scan.setLimit(5);
        scan.withStartRow("00".getBytes());
        scan.withStopRow("1500100010".getBytes());

        ResultScanner scanner = students.getScanner(scan);
        for (Result result : scanner) {

            String rk = Bytes.toString(result.getRow());
            System.out.print(rk + " ");
            for (Cell cell : result.listCells()) {
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                // 列名
                String qua = Bytes.toString(CellUtil.cloneQualifier(cell));
                String cf = Bytes.toString(CellUtil.cloneFamily(cell)); // 列簇名
                if ("age".equals(qua)) {
                    if (Integer.parseInt(value) >= 18) {
                        value = "成年";

                    } else {
                        value = "未成年";
                    }
                }
                System.out.print(value + " ");

            }
            System.out.println();

//            System.out.println(id + "," + name + "," + age + "," + gender + "," + clazz);

        }
    }
    // delete table
    @Test
    public void deleteTable() throws IOException {
        Admin admin = connection.getAdmin();
        if (admin.tableExists(TableName.valueOf("test_cre"))) {
            admin.disableTable(TableName.valueOf("test_cre"));
            admin.deleteTable(TableName.valueOf("test_cre"));
        } else {
            System.out.println("表不存在");
        }
    }
    // create table
    @Test
    public void create() throws IOException {
        Admin admin = connection.getAdmin();
        if (!admin.tableExists(TableName.valueOf("test_cre"))) {
            HTableDescriptor test_cre = new HTableDescriptor(TableName.valueOf("test_cre"));
            HColumnDescriptor cf1 = new HColumnDescriptor("cf1");
            cf1.setMaxVersions(3);
            test_cre.addFamily(cf1);
            admin.createTable(test_cre);
        } else {
            System.out.println("表已存在");
        }

    }
    @After
    public void close() throws IOException {
        connection.close();
    }
}
