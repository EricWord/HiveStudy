package net.codeshow.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * @Description
 * @Author eric
 * @Version V1.0.0
 * @Date 2021/1/8
 */
public class MyUDF extends GenericUDF {
    //    校验数据参数个数
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        if (arguments.length != 1) {
            throw new UDFArgumentException("参数个数不为1");
        }
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    //    处理数据的方法
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
//        1.取出输入数据
        String input = arguments[0].get().toString();
//        2.判断输入数据是否为Null
        if (input == null) {
            return 0;
        }

//3.返回输入数据的长度
        return input.length();
    }

    public String getDisplayString(String[] strings) {
        return "";
    }
}
