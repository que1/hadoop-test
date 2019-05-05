package com.test.hadoop.minmaxcount;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * ProjectName: com.test.hadoop.minmaxcount
 * ClassName:   MinMaxCountMapper
 * Copyright:
 * Company:     xunlei
 * @author:     queyiwen
 * @version:    1.0
 * @since:      jdk 1.7
 * Create at:   2019/4/26
 * Description:
 * <p>
 * <p>
 * Modification History:
 * Date       Author      Version      Description
 * -------------------------------------------------------------
 *
 *
 */
public class MinMaxCountMapper extends Mapper<Object, Text, Text, MinMaxCountTupleWritable> {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private Text outUserId = new Text();
    private MinMaxCountTupleWritable outTupleWritable = new MinMaxCountTupleWritable();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //
        Map<String, String> commentMap = this.transformXmlToMap(value.toString());
        String creationDateStr = commentMap.get("creationDate");
        String userId = commentMap.get("userId");
        Date creationDate = new Date();
        try {
            creationDate = DATE_FORMAT.parse(creationDateStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        this.outTupleWritable.setMin(creationDate);
        this.outTupleWritable.setMax(creationDate);
        this.outTupleWritable.setCount(1L);
        this.outUserId.set(userId);

        context.write(outUserId, outTupleWritable);
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
