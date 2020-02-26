package com.test.hadoop.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * ProjectName: com.test.hadoop.sort
 * ClassName:   TotalSortComparator
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
public class TotalSortComparator extends WritableComparator {

    public TotalSortComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        String[] aKeyValues = a.toString().split(" ");
        int aCloumn1 = Integer.parseInt(aKeyValues[0]);
        int aCloumn2 = Integer.parseInt(aKeyValues[1]);
        String[] bKeyValues = b.toString().split(" ");
        int bCloumn1 = Integer.parseInt(bKeyValues[0]);
        int bCloumn2 = Integer.parseInt(bKeyValues[1]);

        if (aCloumn1 > bCloumn1) {
            return 1;
        } else if (aCloumn1 == bCloumn1){
            if (aCloumn2 >= bCloumn2) {
                return 1;
            } else {
                return -1;
            }
        } else {
            return -1;
        }
    }

}
