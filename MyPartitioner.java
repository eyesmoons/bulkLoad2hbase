package com.enbrands.analyze.spark.Hbase.partitioner;

import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.spark.Partitioner;
import scala.Tuple3;

/**
 * @author shengyu
 * @className
 * @Description
 * @date 2020-08-15 09:29
 **/
public class MyPartitioner extends Partitioner {

    private int numPartitions =  15;

    public MyPartitioner(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    @Override
    public int numPartitions() {
        return numPartitions;
    }

    @Override
    public int getPartition(Object key) {
        //计算region的split键值，总数为partitions-1个
        byte[][] splits = new RegionSplitter.HexStringSplit().split(numPartitions);

        Tuple3<String,String,String> tuple = (Tuple3<String,String,String>) key;

        if (tuple == null) {
            return 0;
        }

        //根据rowkey前缀，计算该条记录属于哪一个region范围内
        int i = 0;
        boolean foundIt = false;

        while (i < splits.length && !foundIt){
            String s = new String(splits[i]);
            if (tuple._1().substring(0,8).compareTo(s) < 0) {
                foundIt = true;
            }
            i++;
        }
        return i;
    }
}
