package com.enbrands.analyze.spark.Hbase.partitioner;

import scala.Tuple3;

import java.io.Serializable;
import java.util.Comparator;
/**
 * @author shengyu
 * @className MyComparator
 * @Description 自定义比较器
 * @date 2020-08-13 09:36
 **/
public class MyComparator implements Serializable, Comparator<Tuple3<String,String, String>> {

    private static final long serialVersionUID = 12382943439484934L;

    public static final MyComparator INSTANCE = new MyComparator();

    private MyComparator(){}

    @Override
    public int compare(Tuple3<String, String, String> tup1, Tuple3<String, String, String> tup2) {
        // 1.根据rowkey排序
        int compare = tup1._1().compareTo(tup2._1());
        if (compare == 0) {
            // 2.根据cf排序
            compare = tup1._2().compareTo(tup2._2());
            if (compare == 0) {
                // 3.根据col排序
                compare = tup1._3().compareTo(tup2._3());
            }
        }
        return compare;
    }
}