package net.codeshow.udf;

import jdk.nashorn.internal.runtime.arrays.ArrayLikeIterator;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description
 * @Author eric
 * @Version V1.0.0
 * @Date 2021/1/8
 */

/**
 * 输入数据:hello,atguigu,hive
 * 输出数据:
 * hello
 * atguigu
 * hive
 */

public class MyUDTF extends GenericUDTF {
    //输出数据的集合
    private ArrayList<String> outputList = new ArrayList<>();

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

//输出数据的默认列名，可以被别名覆盖
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("word");
//        输出数据的类型
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
//        最终返回值
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    //    处理输入数据的方法
    @Override
    public void process(Object[] args) throws HiveException {
//        1.取出输入数据
        String input = args[0].toString();
//传入的分割符
        String splitOp = args[1].toString();
//        2.按照逗号分割字符串
        String[] words = input.split(",");
//        3.遍历数据写出
        for (String word : words) {
//            清空集合
            outputList.clear();
//            将数据放入集合
            outputList.add(word);
//            输出数据
            forward(outputList);
        }


    }

    //   收尾方法
    @Override
    public void close() throws HiveException {

    }
}
