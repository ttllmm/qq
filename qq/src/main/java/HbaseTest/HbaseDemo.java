package HbaseTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HbaseDemo {
    @Test
    public void  createTable() throws Exception {

            Configuration conf=HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum",
                    "hadoop03:2181,hadoop02:2181,hadoop01:2181");

            HBaseAdmin admin = new HBaseAdmin(conf);
            //指定表名
            HTableDescriptor tab1=new HTableDescriptor(TableName.valueOf("tab1"));
            //指定列族名
            HColumnDescriptor colfam1=new HColumnDescriptor("colfam1".getBytes());
            HColumnDescriptor colfam2=new HColumnDescriptor("colfam2".getBytes());
            //指定历史版本存留上限
            colfam1.setMaxVersions(3);

            tab1.addFamily(colfam1);
            tab1.addFamily(colfam2);
            //创建表
            admin.createTable(tab1);

            admin.close();



        }
    @Test
    public void testInsert() throws Exception{
        Configuration conf=HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",
                "hadoop03:2181,hadoop02:2181,hadoop01:2181");
        //尽量复用Htable对象
        HTable table=new HTable(conf,"tab1");
        Put put=new Put("row-1".getBytes());
        //列族，列,值
        put.add("colfam1".getBytes(),"col1".getBytes(),"aaa".getBytes());
        put.add("colfam1".getBytes(),"col2".getBytes(),"bbb".getBytes());
        table.put(put);
        table.close();
    }
    @Test
    public void testInsetrMillion() throws Exception{
        Configuration conf=HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",
                "hadoop03:2181,hadoop02:2181,hadoop01:2181");

        HTable table=new HTable(conf,"tab1");

        List<Put> puts=new ArrayList<Put>();

        long begin=System.currentTimeMillis();

        for(int i=1;i<1000000;i++){
            Put put=new Put(("row"+i).getBytes());
            put.add("colfam1".getBytes(),"col".getBytes(),(""+i).getBytes());
            puts.add(put);

            //批处理，批大小为:10000
            if(i%10000==0){
                table.put(puts);
                puts=new ArrayList<Put>();
            }
        }
        long end=System.currentTimeMillis();
        System.out.println(end-begin);
    }
    @Test
    public void testScan() throws Exception{
        Configuration conf=HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",
                "hadoop03:2181,hadoop02:2181,hadoop01:2181");

        HTable table = new HTable(conf,"tab1");
//获取row100及以后的行键的值
        Scan scan = new Scan("row100".getBytes());
        ResultScanner scanner = table.getScanner(scan);
        Iterator it = scanner.iterator();
        while(it.hasNext()){
            Result result = (Result) it.next();
            byte [] bs = result.getValue(Bytes.toBytes("colfam1"),Bytes.toBytes("col"));
            String str = Bytes.toString(bs);
            System.out.println(str);
        }
        table.close();

    }


    }
