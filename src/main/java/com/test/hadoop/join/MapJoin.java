package com.test.hadoop.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MapJoin extends Configured implements Tool {
    private static final String CUSTOMER_CACHE_URL = "hdfs://yiwen-macbook:9000/test/mapreduce/mapjoin/customer.txt";

    private static class CustomerBean {
        private int custId;
        private String name;
        private String address;
        private String phone;

        public CustomerBean() {

        }

        public CustomerBean(int custId, String name, String address, String phone) {
            this.custId = custId;
            this.name = name;
            this.address = address;
            this.phone = phone;
        }

        public int getCustId() {
            return custId;
        }

        public String getName() {
            return name;
        }

        public String getAddress() {
            return address;
        }

        public String getPhone() {
            return phone;
        }
    }


    private static class CustOrderMapOutKey implements WritableComparable<CustOrderMapOutKey> {
        private int custId;
        private int orderId;

        public void set(int custId, int orderId) {
            this.custId = custId;
            this.orderId = orderId;
        }

        public int getCustId() {
            return custId;
        }

        public int getOrderId() {
            return orderId;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(this.custId);
            out.writeInt(this.orderId);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.custId = in.readInt();
            this.orderId = in.readInt();
        }

        @Override
        public int compareTo(CustOrderMapOutKey o) {
            int res = Integer.compare(this.custId, o.custId);
            return res == 0 ? Integer.compare(this.orderId, o.orderId) : res;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof CustOrderMapOutKey) {
                CustOrderMapOutKey o = (CustOrderMapOutKey)obj;
                return this.custId == o.custId && orderId == o.orderId;
            } else {
                return false;
            }
        }

        @Override
        public String toString() {
            return this.custId + "\t" + this.orderId;
        }
    }

    private static class JoinMapper extends Mapper<LongWritable, Text, CustOrderMapOutKey, Text> {
        private final CustOrderMapOutKey outputKey = new CustOrderMapOutKey();
        private final Text outputValue = new Text();

        /**
         * 在内存中customer数据
         */
        private static final Map<Integer, CustomerBean> CUSTOMER_MAP = new HashMap<Integer, CustomerBean>();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // 格式: 订单编号 客户编号 订单金额
            String[] cols = value.toString().split("\t");
            if (cols.length < 3) {
                return;
            }

            int custId = Integer.parseInt(cols[1]);     // 取出客户编号
            CustomerBean customerBean = CUSTOMER_MAP.get(custId);

            if (customerBean == null) {         // 没有对应的customer信息可以连接
                return;
            }

            StringBuffer sb = new StringBuffer();
            sb.append(cols[2])
                    .append("\t")
                    .append(customerBean.getName())
                    .append("\t")
                    .append(customerBean.getAddress())
                    .append("\t")
                    .append(customerBean.getPhone());

            this.outputKey.set(custId, Integer.parseInt(cols[0]));
            this.outputValue.set(sb.toString());

            context.write(this.outputKey, this.outputValue);
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSystem fs = FileSystem.get(URI.create(CUSTOMER_CACHE_URL), context.getConfiguration());
            FSDataInputStream fdis = fs.open(new Path(CUSTOMER_CACHE_URL));

            BufferedReader reader = new BufferedReader(new InputStreamReader(fdis));
            String line = null;
            String[] cols = null;

            // 格式：客户编号  姓名  地址  电话
            while ((line = reader.readLine()) != null) {
                cols = line.split("\t");
                if (cols.length < 4) {              // 数据格式不匹配，忽略
                    continue;
                }

                CustomerBean bean = new CustomerBean(Integer.parseInt(cols[0]), cols[1], cols[2], cols[3]);
                CUSTOMER_MAP.put(bean.getCustId(), bean);
            }
        }
    }


    private static class JoinReducer extends Reducer<CustOrderMapOutKey, Text, CustOrderMapOutKey, Text> {
        @Override
        protected void reduce(CustOrderMapOutKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 什么事都不用做，直接输出
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            new IllegalArgumentException("Usage: <inpath> <outpath>");
            return;
        }

        ToolRunner.run(new Configuration(), new MapJoin(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, MapJoin.class.getSimpleName());
        job.setJarByClass(MapJoin.class);
        // map settings
        job.setMapperClass(JoinMapper.class);
        job.setMapOutputKeyClass(CustOrderMapOutKey.class);
        job.setMapOutputValueClass(Text.class);
        // reduce settings
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(CustOrderMapOutKey.class);
        job.setOutputKeyClass(Text.class);

        // 添加customer cache文件
        job.addCacheFile(URI.create(CUSTOMER_CACHE_URL));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean res = job.waitForCompletion(true);

        return res ? 0 : 1;
    }

}
