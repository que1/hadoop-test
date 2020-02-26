package com.test.hadoop.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * ProjectName: com.test.hadoop.sort
 * ClassName:   KeyPartitioner
 * Copyright:
 * Company:     xunlei
 * @author:     queyiwen
 * @version:    1.0
 * @since:      jdk 1.7
 * Create at:   2020/2/26
 * Description:
 * <p>
 * <p>
 * Modification History:
 * Date       Author      Version      Description
 * -------------------------------------------------------------
 *
 *
 *
 */
public class TotalSortKeyPartitioner extends Partitioner<Text, NullWritable> {

    @Override
    public int getPartition(Text text, NullWritable nullWritable, int numPartitions) {
        int keyCloumn1 = Integer.parseInt(text.toString().split(" ")[0]);
        if (keyCloumn1 < 9) {
            return 0;
        } else {
            return 1;
        }
    }

}
