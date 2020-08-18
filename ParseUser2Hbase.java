package com.enbrands.analyze.spark.Hbase;

import com.alibaba.fastjson.JSONObject;
import com.enbrands.UDF;
import com.enbrands.analyze.SparkJob;
import com.enbrands.analyze.constants.ImpalaTable;
import com.enbrands.analyze.spark.Hbase.beans.SparkColumn;
import com.enbrands.analyze.spark.Hbase.partitioner.MyComparator;
import com.enbrands.analyze.spark.Hbase.partitioner.MyPartitioner;
import com.enbrands.analyze.util.SparkRowUtil;
import com.enbrands.shark.common.Context;
import com.enbrands.shark.common.hbase.user.UserBaseFields;
import com.enbrands.shark.common.hbase.user.UserModelFields;
import com.enbrands.shark.common.util.TimeUtil;
import com.enbrands.udf.BuyCharacteristics;
import com.enbrands.udf.BuySubdivision;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.util.*;
import org.apache.spark.sql.functions;

/**
 * 将用户数据全量同步到Hbase,用于补数据。
 */
public class ParseUser2Hbase extends SparkJob {

    //当天，调度时间减一天
    private String today;

    //过去180天
    private String formerlyDay360;

    //过去90天
    private String formerlyDay90;

    private String temp_members_table = "temp_member_table";

    private String temp_order_table = "temp_order_table";

    private String temp_city_level = "temp_city_level";

    private String temp_streaming_t_hd_join_log;

    private String temp_streaming_t_right_change_log;

    private static String base_cf = "base";

    private static String model_cf = "model";

    private static int numPartitions = 15;

    private List<String> base_list;

    private List<String> model_list;

    public ParseUser2Hbase(String[] args) {
        super(args);
    }

    @Override
    public void setup() {
        today = TimeUtil.dayStrToHiveDay(time);
        today = "2020-08-01";
        formerlyDay360 = TimeUtil.dayStrToHiveDay(TimeUtil.minusTime(time, 359, Calendar.DAY_OF_MONTH, time_zone));
        formerlyDay90 = TimeUtil.dayStrToHiveDay(TimeUtil.minusTime(time, 89, Calendar.DAY_OF_MONTH, time_zone));
        sqlContext.udf().register(UDF.BUY_CHARACTERISTICS.name(), new BuyCharacteristics(), DataTypes.IntegerType);
        sqlContext.udf().register(UDF.BUY_SUBDIVISION.name(), new BuySubdivision(), DataTypes.IntegerType);
        //会员表
        executeSQL("select * from dws_customer.member_base_info_day").where(merchantSql).where(String.format("day_id = '%s'", today)).createOrReplaceTempView(temp_members_table);
        //互动参与表
        temp_streaming_t_hd_join_log = loadImpalaTable(odsDataBase, ImpalaTable.STREAMING_T_HD_JOIN_LOG, true);
        temp_streaming_t_right_change_log = loadImpalaTable(odsDataBase, ImpalaTable.STREAMING_T_RIGHT_CHANGE_LOGS, true);
        //订单表
        executeSQL("select * from dwd_trade.user_orders").where(merchantSql).where(String.format("day_id = '%s'", today)).createOrReplaceTempView(temp_order_table);
        //城市等级表
        executeSQL("select name,cast(level as int) as level from dim_common.common_city_level").createOrReplaceTempView(temp_city_level);

        // 缓存base列族的column
        base_list = UserBaseFields.nameList();
        // 缓存model嫘祖的column
        model_list = UserModelFields.nameList();

        // 指定类使用kryo序列化
        sparkContext.getConf().registerKryoClasses(new Class[]{MyComparator.class});
        sparkContext.getConf().registerKryoClasses(new Class[]{MyPartitioner.class});
        sparkContext.getConf().registerKryoClasses(new Class[]{SparkColumn.class});
    }

    @Override
    public void handle() throws Throwable {
        printInfoLog("开始统计会员信息。");
        Dataset<Row> memberRows = getMembers();

        //在订单中统计非会员信息，能拿到的信息有限
        printInfoLog("开始统计非会员信息。");
        Dataset<Row> unMemberRows = getUnMembers();

        //全量的会员
        Dataset<Row> allMemberRows = memberRows.union(unMemberRows).distinct();

        //开始打标签
        printInfoLog("开始给用户打标签。");
        Dataset<Row> resultRows = getResultRows(allMemberRows);

        saveAsHfile(resultRows);

        logger.info("数据清洗完毕");
    }

    /**
     * @Description: 保存dataFrame为Hfile
     * @Author: shengyu
     * @Date: 2020/8/13/013
     **/
    public void saveAsHfile(Dataset<Row> df) throws Exception {
        String hfile_path = "hdfs://sky-ns/test/hfile";
        String[] rowKey_fileds = {"user_id", "merchant_num"};
        String hBase_table = "test_users";

        // zk相关
        String quorum = Context.ZOOKEEPER;
        String clientPort = "2181";

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", quorum);
        conf.set("hbase.zookeeper.property.clientPort", clientPort);
        conf.set(TableOutputFormat.OUTPUT_TABLE, hBase_table);

        Job job = Job.getInstance(conf);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);

        // 设置输出为snappy压缩
        sparkContext.getConf().set("spark.hadoop.mapred.output.compress","true");
        sparkContext.getConf().set("spark.hadoop.mapred.output.compression.codec","org.apache.hadoop.io.compress.snappy");

        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf(hBase_table.getBytes());

        // 表不存在则创建hbase临时表
        creteHTable(hBase_table,conf);

        HFileOutputFormat2.configureIncrementalLoad(job, conn.getTable(tableName), conn.getRegionLocator(tableName));

        // 删除已经存在的临时路径，否则会报错
        delete_hdfspath(hfile_path);

        // 转换结构为List<<<rowkey,cf,column>,value>>
        JavaRDD<List<Tuple2<Tuple3<String, String, String>, String>>> mapRdd = df.toJavaRDD().map((Function<Row, List<Tuple2<Tuple3<String, String, String>, String>>>) row -> {
            List<Tuple2<Tuple3<String, String, String>, String>> arrayList = new ArrayList<>();
            Map<String, String> columnsDataMap = getColumnsDataMap(row);
            String rowKey = SparkRowUtil.getRowkey(columnsDataMap, rowKey_fileds);

            Iterator<Map.Entry<String, String>> iterator = columnsDataMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, String> next = iterator.next();
                String col = next.getKey().toString();
                String value = next.getValue().toString();
                String  cf = getColumnFamilyByColumn(col.toLowerCase());
                Tuple2<Tuple3<String, String, String>, String> tuple22 = new Tuple2<>(new Tuple3<>(rowKey,cf,col), value);
                arrayList.add(tuple22);
            }
            return arrayList;
        });

        mapRdd.cache();
        mapRdd.isEmpty();

        // 拆分为<<rowkey,cf,column>,value>格式
        JavaPairRDD<Tuple3<String, String, String>, String> flatMapRdd =
                mapRdd.filter(item->!item.isEmpty())
                      .flatMapToPair((PairFlatMapFunction<List<Tuple2<Tuple3<String, String, String>, String>>, Tuple3<String, String, String>, String>) tuple2s -> tuple2s.iterator());

        // 对<rowkey,cf,column>进行二次排序
        JavaPairRDD<Tuple3<String, String, String>, String> sortedRdd = flatMapRdd.repartitionAndSortWithinPartitions(new MyPartitioner(numPartitions), MyComparator.INSTANCE);

        // 将排序完成的rdd转换为hfile格式<ImmutableBytesWritable,keyValue>
        JavaPairRDD<ImmutableBytesWritable, KeyValue> hfileRdd = sortedRdd.mapToPair((PairFunction<Tuple2<Tuple3<String, String, String>, String>, ImmutableBytesWritable, KeyValue>) tuple -> {
            byte[] row_key_byte = Bytes.toBytes(tuple._1()._1());
            byte[] cf_byte = Bytes.toBytes(tuple._1()._2());
            byte[] qua_byte = Bytes.toBytes(tuple._1()._3());
            byte[] value_byte = Bytes.toBytes(tuple._2());

            KeyValue keyValue = new KeyValue(row_key_byte, cf_byte, qua_byte, value_byte);
            return new Tuple2<>(new ImmutableBytesWritable(row_key_byte), keyValue);
        });

        // 保存为Hfile
        hfileRdd.saveAsNewAPIHadoopFile(hfile_path, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, job.getConfiguration());

        setPermission(hfile_path);

        // 加载hfile到hbase表中
        new LoadIncrementalHFiles(conf).doBulkLoad(new Path(hfile_path),conn.getAdmin(),conn.getTable(tableName),conn.getRegionLocator(tableName));
    }

    /**
     * @Description: 根据列名获取列族
     * @Author: shengyu
     * @Date: 2020/8/15/015
     **/
    private String getColumnFamilyByColumn(String col){
        String cf = "";
        if (base_list.contains(col)) {
            cf = base_cf;
        } else if (model_list.contains(col)) {
            cf = model_cf;
        }
        return cf;
    }
    /**
     * @Description: 根据row封装成Map类型的数据
     * @Author: shengyu
     * @Date: 2020/8/15/015
     **/
    private Map<String, String> getColumnsDataMap(Row row) {
        List<SparkColumn> columnsList = new ArrayList<>();
        StructField[] fields = row.schema().fields();
        for(StructField f : fields) {
            SparkColumn column = new SparkColumn();
            column.setName(f.name());
            column.setDataType(f.dataType());
            columnsList.add(column);
        }

        // 遍历hbase列，获取列值，得到columnsDataMap
        Map<String, String> columnsDataMap = new HashMap<>();
        for(SparkColumn column : columnsList) {
            String strValue = getColumnValue(column, row);
            if(strValue != null) {
                columnsDataMap.put(column.getLowerCaseName(), strValue);
            }
        }
        return columnsDataMap;
    }

    /**
     * @Description: 获取列值，并转换为字符串
     * @Author: shengyu
     * @Date: 2020/8/13/013
     **/
    private String getColumnValue(SparkColumn column, Row r) {
        if(column.getDataType() instanceof ArrayType) {
            List<Object> listValue = SparkRowUtil.getListCell(r, column.getName());
            return listValue == null ? null : JSONObject.toJSONString(listValue);
        } else if (column.getDataType() instanceof MapType) {
            Map<String, Object> mapValue = SparkRowUtil.getMapCell(r, column.getName());
            return mapValue == null ? null : JSONObject.toJSONString(mapValue);
        } else {
            Object objValue = SparkRowUtil.getObjectCell(r, column.getName());
            return objValue == null ? null : (objValue + "");
        }
    }
    /**
     * @Description: 删除hdfs临时路径
     * @Author: shengyu
     * @Date: 2020/8/13/013
     **/
    private void delete_hdfspath(String save_path) throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        Path path = new Path(save_path);
        if (fs.exists(path)) {
            fs.delete(path,true);
        }
    }

    /**
     * @Description: 给hdfs指定目录设置权限
     * @Author: shengyu
     * @Date: 2020/8/18/018
     **/
    private void setPermission(String filePath) throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        Path path = new Path(filePath);
        FsPermission permission = new FsPermission(FsAction.ALL,FsAction.ALL, FsAction.ALL);
        if (fs.exists(path)) {
            fs.setPermission(path,permission);
            RemoteIterator<LocatedFileStatus> fileList = fs.listFiles(path, true);
            while (fileList.hasNext()) {
                LocatedFileStatus next = fileList.next();
                Path path1 = next.getPath();
                fs.setPermission(path1,permission);
            }
        }
    }

    /**
     * @Description: 创建hbase表
     * @Author: shengyu
     * @Date: 2020/8/13/013
     **/
    private void creteHTable(String tableName, Configuration hBaseConf) throws IOException {
        Connection connection = ConnectionFactory.createConnection(hBaseConf);
        TableName hBaseTableName = TableName.valueOf(tableName);
        Admin admin = connection.getAdmin();
        if (!admin.tableExists(hBaseTableName)) {
            HTableDescriptor tableDesc = new HTableDescriptor(hBaseTableName);
            HColumnDescriptor base_hcd = new HColumnDescriptor(Bytes.toBytes(base_cf));
            HColumnDescriptor model_hcd = new HColumnDescriptor(Bytes.toBytes(model_cf));

            tableDesc.addFamily(base_hcd.setCompressionType(Compression.Algorithm.SNAPPY));
            tableDesc.addFamily(model_hcd.setCompressionType(Compression.Algorithm.SNAPPY));
            byte[][] splits = new RegionSplitter.HexStringSplit().split(numPartitions);
            admin.createTable(tableDesc,splits);
        }
        numPartitions = admin.getTableRegions(hBaseTableName).size();
        connection.close();
    }

    /**
     * 给用户打标签
     *
     * @param allMemberRows 所有用户
     * @return 全量用户画像
     */
    private Dataset<Row> getResultRows(Dataset<Row> allMemberRows) {
        //订单
        Dataset<Row> orderRows = getOrder();
        //参加活动
        Dataset<Row> joinRows = getJoin();
        Dataset<Row> changeLogRows = getChangeLog();
        //会员left join订单left join活动
        allMemberRows.join(orderRows,
                JavaConverters.asScalaIteratorConverter(Arrays.asList("merchant_num", "user_id").iterator()).asScala().toSeq(),
                "left").join(functions.broadcast(joinRows),
                JavaConverters.asScalaIteratorConverter(Arrays.asList("merchant_num", "member_id").iterator()).asScala().toSeq(),
                "left").join(functions.broadcast(changeLogRows),
                JavaConverters.asScalaIteratorConverter(Arrays.asList("merchant_num", "member_id").iterator()).asScala().toSeq(),
                "left")
                .select(allMemberRows.col("merchant_num"), allMemberRows.col("brand_code"),
                        allMemberRows.col("platform_type"), allMemberRows.col("user_id"), allMemberRows.col("nick"),
                        allMemberRows.col("platform_open_id"), allMemberRows.col("user_name"), allMemberRows.col("is_member"),
                        allMemberRows.col("member_id"), allMemberRows.col("sex"), allMemberRows.col("birthday"),
                        allMemberRows.col("age"), allMemberRows.col("phone"), allMemberRows.col("mix_mobile"),
                        allMemberRows.col("status"), allMemberRows.col("register_time"), allMemberRows.col("unbind_time"),
                        allMemberRows.col("channel"), allMemberRows.col("member_identity"), allMemberRows.col("province"),
                        allMemberRows.col("city"),allMemberRows.col("city_level"), allMemberRows.col("district"), allMemberRows.col("address"),
                        allMemberRows.col("point"), allMemberRows.col("member_grade"), allMemberRows.col("is_new_member"),
                        allMemberRows.col("is_loss_member"), allMemberRows.col("receive_province"), allMemberRows.col("receive_province_code"),
                        allMemberRows.col("receive_city"), allMemberRows.col("receive_city_code"),
                        allMemberRows.col("chinese_zodiac"), allMemberRows.col("zodiac"),
                        allMemberRows.col("is_have_phone"), allMemberRows.col("is_have_baby"),
                        allMemberRows.col("baby_age"), allMemberRows.col("medal_list"),
                        allMemberRows.col("is_focus"), allMemberRows.col("tags_list"),
                        allMemberRows.col("member_point_end_time"), allMemberRows.col("member_grade_end_time"),
                        allMemberRows.col("medal_end_time"), allMemberRows.col("member_ship_num"), allMemberRows.col("receive_msg_num"),
                        orderRows.col("history_order_num_360"),
                        orderRows.col("today_order_num"), orderRows.col("today_order_amount"), orderRows.col("pay_detail"),
                        joinRows.col("join_num_90"), joinRows.col("exchange_time_list"), joinRows.col("exchange_num"), joinRows.col("interactive_num"),
                        joinRows.col("interactive_time_list"), changeLogRows.col("total_point")).distinct()
                .registerTempTable("result");
        //打标签:
        //buy_charact int comment '购买特征（0:未购买 1：首购会员购买 2：复购会员购买 3：非会员购买）',
        //buy_subdivision int comment '购买细分（0:未知 1：当日入会且购买 2：非当日入会首购）',
        //is_silent int comment '非沉默标签(0:沉默 1:非沉默)'
        //pay_detail int comment '购买细节（0：未购买 1：在店铺成功购买，购买天数=1天 2：在店铺成功购买，购买天数>=2天）'
        return executeSQL(String.format("select cast(merchant_num as int) as merchant_num, brand_code, platform_type, user_id, nick, platform_open_id, user_name, " +
                "is_member, member_id, sex, birthday, age, phone, mix_mobile, cast(status as int) as status, register_time, unbind_time, cast(channel as int) as channel, " +
                "cast(member_identity as int) as member_identity, province, city, city_level, district, address, cast(point as int) as point,cast(total_point as int) as total_point, cast(member_grade as int) as member_grade, " +
                "is_new_member, is_loss_member,receive_province,cast(receive_province_code as int) as receive_province_code,receive_city,cast(receive_city_code as int) as receive_city_code, if(today_order_num is not null, today_order_num, 0) as today_order_num, " +
                "if(today_order_amount is not null, cast(today_order_amount as decimal(15,3)), cast(0 as decimal(15,3))) as today_order_amount, " +
                //计算购买特征
                "buy_characteristics(if(is_member = 1, true, false), if(is_new_member = 1 , true, false), today_order_num, history_order_num_360) as buy_charact, " +
                //计算购买细分
                "buy_subdivision(if(is_member = 1, true, false), if(is_new_member = 1 , true, false), today_order_num, history_order_num_360) as  buy_subdivision, " +
                //分沉默标签
                "if(join_num_90 >= 1 and member_id > 1, 1, 0) as is_silent, " +
                //购买细节标签
                "if(pay_detail is null,0,pay_detail) as pay_detail, " +
                "exchange_time_list,cast(exchange_num as int) as exchange_num,cast(interactive_num as int) as interactive_num,interactive_time_list, " +
                "chinese_zodiac,zodiac,is_have_phone,is_have_baby,baby_age,medal_list,is_focus,tags_list," +
                "member_point_end_time,member_grade_end_time,medal_end_time,member_ship_num,receive_msg_num " +
                "from result where %s", merchantSql)).distinct();
    }


    /**
     * 获取参加活动的数据全部
     *
     * @return 活动数据集
     */
    private Dataset<Row> getJoin() {
        return executeSQL(String.format("select a.merchant_num,a.member_id,b.exchange_time_list,b.exchange_num, " +
                        "c.join_num_90,c.interactive_num,d.interactive_time_list from " +
                        "(select merchant_num,member_id from %s group by merchant_num,member_id) a " +
                        "left join " +
                        "(select merchant_num,concat_ws(',', collect_set(substr(created_at, 1, 19))) as exchange_time_list,member_id,count(if(status = 1,1,null)) as exchange_num " +
                        "from %s where activity_type = 2 group by merchant_num,member_id) b " +
                        "on a.merchant_num = b.merchant_num and a.member_id = b.member_id " +
                        "left join " +
                        "(select merchant_num, member_id, 1 as join_num_90,count(if(status = 1,1,null)) as interactive_num from %s " +
                        "where substr(created_at, 1, 10) between '%s' and '%s' group by merchant_num,member_id) c " +
                        "on a.merchant_num = c.merchant_num and a.member_id = c.member_id " +
                        "left join " +
                        "(select merchant_num,concat_ws(',', collect_set(substr(created_at, 1, 19))) as interactive_time_list,member_id " +
                        "from %s group by merchant_num,member_id) d " +
                        "on a.merchant_num = d.merchant_num and a.member_id = d.member_id ",
                temp_streaming_t_hd_join_log,
                temp_streaming_t_hd_join_log,
                temp_streaming_t_hd_join_log, formerlyDay90, today,
                temp_streaming_t_hd_join_log));
    }

    /**
     * 积分变化
     *
     * @return 积分变化数据集
     */
    private Dataset<Row> getChangeLog() {
        return executeSQL(String.format("select merchant_num, " +
                        "member_id, " +
                        "sum(nvl(num_change, 0)) total_point " +
                        "from %s " +
                        "where num_change > 0 " +
                        "and right_type = 1 " +
                        "group by merchant_num, member_id",
                temp_streaming_t_right_change_log));
    }

    /**
     * 获取订单数据
     *
     * @return 订单数据集
     */
    private Dataset<Row> getOrder() {
        String orderSql = "select a.merchant_num, a.user_id, a.order_num_360, (a.order_num_360 - b.today_order_num) as history_order_num_360, b.today_order_num, b.today_order_amount,cast(c.pay_detail as int) as pay_detail from " +
                "(select merchant_num, platform_open_id as user_id, count(1) as order_num_360 from %s " +
                "where substr(pay_at, 1, 10) between '%s' and '%s' and status in (3, 4, 5, 6,101) group by merchant_num, user_id) a " +
                "left join " +
                "(select merchant_num, platform_open_id as user_id, count(1) as today_order_num, sum(payment) as today_order_amount from %s " +
                "where substr(pay_at, 1, 10) = '%s' and status in (3, 4, 5, 6,101) group by merchant_num, user_id) b " +
                "on " +
                "a.merchant_num = b.merchant_num and a.user_id = b.user_id " +
                "left join " +
                "(select t.merchant_num, t.platform_open_id as user_id,if(max(t.rn) >1,2,1) as pay_detail " +
                "from (" +
                "select merchant_num, platform_open_id, " +
                "rank() over(partition by  merchant_num, platform_open_id order by substr(pay_at, 1, 10) desc) as rn " +
                "from %s where substr(pay_at, 1, 10) between '%s' and '%s' and status in (3, 4, 5, 6,101)) t  group by t.merchant_num, t.platform_open_id) c " +
                "on " +
                "a.merchant_num = c.merchant_num and a.user_id = c.user_id ";

        return executeSQL(String.format(orderSql,
                temp_order_table, formerlyDay360, today,
                temp_order_table, today,
                temp_order_table, formerlyDay360, today)).distinct();
    }

    /**
     * 获取非会员信息
     *
     * @return 非会员表
     */
    private Dataset<Row> getUnMembers() {
        return executeSQL(String.format(
                //商家编号、品牌编号、平台类型、用户ID
                "select a.merchant_num, a.brand_code, a.platform_type, a.user_id, " +
                        "a.nick, a.platform_open_id, a.user_name, a.is_member, a.member_id, " +
                        "a.sex, a.birthday, a.age, a.phone, a.mix_mobile,a.status, " +
                        "a.register_time, a.unbind_time, a.channel, a.member_identity, " +
                        "a.province, a.city,a.city_level, a.district, a.address, a.point, a.member_grade, a.is_new_member,a.is_loss_member, " +
                        "a.receive_province,a.receive_province_code,a.receive_city,a.receive_city_code, " +
                        "a.chinese_zodiac,a.zodiac,a.is_have_phone,a.is_have_baby,a.baby_age,a.medal_list,a.is_focus,a.tags_list," +
                        "a.member_point_end_time,a.member_grade_end_time,a.medal_end_time,a.member_ship_num,a.receive_msg_num " +
                        "from " +
                        "(select merchant_num, brand_code, platform_type, platform_open_id as user_id, " +
                        //用户昵称，用户平台ID，用户姓名，是否会员，会员ID
                        "nick, platform_open_id, '' as user_name, is_member, 0 as member_id, " +
                        //性别，生日、年龄、联系电话、混淆手机号
                        "0 as sex, '0001-01-01' as birthday, 0 as age, '' as phone, '' as mix_mobile, " +
                        //状态、注册时间、解绑时间、渠道、会员身份
                        "1 as status, '0001-01-01 00:00:00' as register_time, '0001-01-01 00:00:00' as unbind_time, 6 as channel, 0 as member_identity, " +
                        //省、市、城市等级、区/县、详细住址、积分、会员等级、是否当日新增会员、是否当日解绑会员
                        "'' as province, '' as city, -1 as city_level, '' as district, '' as address, 0 as point, 0 as member_grade, 0 as is_new_member, 0 as is_loss_member, " +
                        //收货人所在省编码、收货人所在省、收货人所在市编码、收货人所在城市
                        "province as receive_province,province_code as receive_province_code,city as receive_city,city_code as receive_city_code, " +
                        //肖像、星座、手机号码是否完善、拥有宝宝、宝宝年龄、会员勋章、关注店铺、商家给用户打的标签列表
                        "'未知' as chinese_zodiac,'未知' as zodiac,0 as is_have_phone,0 as is_have_baby,0 as baby_age,'' as medal_list,0 as is_focus,'' as tags_list," +
                        //会员积分过期日期、会员等级过期日期、会员勋章过期日期、入会次数、接收短信次数
                        "'0001-01-01 00:00:00' as member_point_end_time,'0001-01-01 00:00:00' as member_grade_end_time,'0001-01-01 00:00:00' as medal_end_time,0 as member_ship_num,0 as receive_msg_num, " +
                        "row_number() over(partition by merchant_num, platform_open_id order by updated_at desc) as rn " +
                        "from %s " +
                        "where is_member = 0 ) a where a.rn = 1", temp_order_table));
    }

    /**
     * 获取会员信息
     *
     * @return 会员表
     */
    private Dataset<Row> getMembers() {
        executeSQL(String.format(
                //商家编号、品牌编号、平台类型、用户ID
                "select a.merchant_num,a.user_id,a.receive_province,a.receive_province_code,a.receive_city,a.receive_city_code from (" +
                        "select merchant_num, platform_open_id as user_id, " +
                        //收货人所在省编码、收货人所在省、收货人所在市编码、收货人所在城市
                        "province as receive_province,province_code as receive_province_code,city as receive_city,city_code as receive_city_code, " +
                        "row_number() over(partition by merchant_num, platform_open_id order by updated_at desc) as rn " +
                        "from %s " +
                        "where is_member = 1) a where a.rn = 1", temp_order_table)).createOrReplaceTempView("t_temp_order");
        return executeSQL(String.format(
                //品牌编号、商家编号、平台类型、用户ID
                "select a.merchant_num,a.brand_code, a.platform_type, a.user_id, " +
                        //用户昵称，用户平台ID，用户姓名，是否会员，会员ID，性别，生日
                        "a.nick, a.platform_open_id, a.user_name,1 as is_member, a.member_id, a.sex, if(a.birthday = '','0001-01-01',substr(a.birthday, 1, 10)) as birthday, " +
                        //年龄、联系电话、混淆手机号、状态、注册时间、解绑时间、渠道、
                        "a.age, a.phone, a.mix_mobile, a.status, a.register_time, if(a.unbind_time = '','0001-01-01 00:00:00',a.unbind_time) as unbind_time, a.channel, " +
                        //会员身份、省、市、城市等级、区/县、详细住址、积分、会员等级
                        "a.member_identity, a.province, replace(a.city,'市','') as city, c.level as city_level, a.district, a.address, a.point, a.member_grade, " +
                        //是否当日新增会员、是否当日解绑会员
                        "a.is_new_member, a.is_loss_member, " +
                        //收货人所在省编码、收货人所在省、收货人所在市编码、收货人所在城市
                        "b.receive_province,b.receive_province_code,b.receive_city,b.receive_city_code," +
                        //肖像、星座、手机号码是否完善、拥有宝宝、宝宝年龄、会员勋章、关注店铺、商家给用户打的标签列表
                        "a.chinese_zodiac,a.zodiac,a.is_have_phone,a.is_have_baby,a.baby_age,a.medal_list,a.is_focus,a.tags_list," +
                        //会员积分过期日期、会员等级过期日期、会员勋章过期日期、入会次数、接收短信次数
                        "a.member_point_end_time,a.member_grade_end_time,a.medal_end_time,a.member_ship_num,a.receive_msg_num " +
                        "from %s as a " +
                        "left join %s as b " +
                        "on a.merchant_num = b.merchant_num and a.user_id = b.user_id " +
                        "left join %s as c " +
                        "on replace(a.city,'市','') = c.name", temp_members_table, "t_temp_order", temp_city_level));
    }

    @Override
    public void cleanup() {

    }

    public static void main(String[] args) {
        new ParseUser2Hbase(args).run();
    }
}