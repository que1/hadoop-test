package com.test.hadoop.average;

import com.alibaba.fastjson.JSON;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * ProjectName: com.test.hadoop.average
 * ClassName:   AverageMapper
 * Copyright:
 * Company:     xunlei
 * @author:     queyiwen
 * @version:    1.0
 * @since:      jdk 1.7
 * Create at:   2019/5/6
 * Description:
 * <p>
 * <p>
 * Modification History:
 * Date       Author      Version      Description
 * -------------------------------------------------------------
 */
public class AverageMapper extends Mapper<Object, Text, IntWritable, CountAverageTupleWritable> {

    private final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private IntWritable outHour = new IntWritable();
    private CountAverageTupleWritable countAverageTupleWritable = new CountAverageTupleWritable();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //
        Map<String, String> commentMap = this.transformXmlToMap(value.toString());
        String strDate = commentMap.get("creationDate");
        String text = commentMap.get("text");

        Date creationDate = new Date();
        try {
            creationDate = DATE_FORMAT.parse(strDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(creationDate);
        this.outHour.set(calendar.get(Calendar.HOUR_OF_DAY));

        this.countAverageTupleWritable.setCount(1);
        this.countAverageTupleWritable.setAverage(text == null ? 0 : text.length());

        context.write(this.outHour, this.countAverageTupleWritable);
    }


    public Map<String, String> transformXmlToMap(String valueStr) {
        Map<String, String> commentMap = new HashMap<String, String>();
        try {
            commentMap = (Map) JSON.parseObject(valueStr);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return commentMap;
    }

}
