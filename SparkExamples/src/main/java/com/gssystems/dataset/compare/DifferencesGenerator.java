package com.gssystems.dataset.compare;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;

public class DifferencesGenerator implements FlatMapFunction<Row, Row> {
    List<String> keyFields = null;
    String leftDataset = null;
    String rightDataset = null;

    public DifferencesGenerator(String[] keys, String leftDs, String rightDS) {
        keyFields = Arrays.asList(keys);
        leftDataset = leftDs;
        rightDataset = rightDS;
    }

    @Override
    public Iterator<Row> call(Row t) throws Exception {
        ArrayList<Row> toRet = new ArrayList<Row>();

        //For every non key col, we need to compare and emit a row.
        StructType schema = t.schema();
        String[] fieldNames = schema.fieldNames();

        for( String aCol: fieldNames ) {
            //Does col belong to keys? Ignore
            if( keyFields.contains(aCol)) {
                continue;
            }

            //If field ends with _r ignore
            if( aCol.endsWith("_r")) {
                continue;
            }

            //Get regular col, the _r column from the joined data, compare and emit row!

            Object[] fields = new Object[6];
            fields[0] = leftDataset;
            fields[1] = rightDataset;
            fields[2] = aCol;

            Object leftVal = t.getAs(aCol).toString();
            Object rightVal = t.getAs(aCol + "_r").toString();

            fields[3] = leftVal;
            fields[4] = rightVal;
            fields[5] = leftVal.equals(rightVal);

            toRet.add(RowFactory.create(fields));
        }

        return toRet.iterator();
    }

}
