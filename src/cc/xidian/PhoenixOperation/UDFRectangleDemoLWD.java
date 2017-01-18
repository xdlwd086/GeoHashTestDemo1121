package cc.xidian.PhoenixOperation;


import java.util.List;

import cc.xidian.GeoObject.RectangleQueryScope;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarchar;

@BuiltInFunction(name = UDFRectangleDemoLWD.NAME, args = {@Argument(allowedTypes={PDouble.class, PDouble.class, PVarchar.class})})//
public class UDFRectangleDemoLWD extends ScalarFunction {
	
	public static final String NAME = "within";
	
	//Log log = LogFactory.getLog(UDFJTSDemo.class);
	public UDFRectangleDemoLWD() {
	}

	public UDFRectangleDemoLWD(List<Expression> children) {
		super(children);
	}

	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		//3个参数
		Expression arg0 = getChildren().get(0);
		Expression arg1 = getChildren().get(1);
		Expression arg2 = getChildren().get(2);

		//第一个参数，对应点的横坐标x，int型
		if(!arg0.evaluate(tuple, ptr)  ){
			return false;
		}
		//int iarg1 = (int) arg1.getDataType().toObject(ptr,arg1.getDataType());
		double xLongitudeUDF = (Double)arg0.getDataType().toObject(ptr,arg0.getDataType());
		//第二个参数，对应点的纵坐标y，int型
		if(!arg1.evaluate(tuple, ptr)){
			return false;
		}
		//int iarg2 = (int) arg2.getDataType().toObject(ptr, arg2.getDataType());
		double yLatitudeUDF = (Double)arg1.getDataType().toObject(ptr,arg1.getDataType());
		//第三个参数，对应多边形的顶点，WKT格式，String型
		if(!arg2.evaluate(tuple, ptr)){
			return false;
		}
		String rectangleQueryScopeUDF = (String) arg2.getDataType().toObject(ptr, arg2.getDataType());



		//log.info("----------- x:"+xLongitudeUDF+"  y:"+yLatitudeUDF+" str:"+rectangleQueryScopeUDF);

		try {
			String[] rQSUArray = rectangleQueryScopeUDF.split(",");
			double xLongitudeBLUDF = Double.parseDouble(rQSUArray[0].trim());
			double yLatitudeBLUDF = Double.parseDouble(rQSUArray[1].trim());
			double xLongitudeTRUDF = Double.parseDouble(rQSUArray[2].trim());
			double yLatitudeTRUDF = Double.parseDouble(rQSUArray[3].trim());
			RectangleQueryScope rQSUDF = new RectangleQueryScope(xLongitudeBLUDF,yLatitudeBLUDF,xLongitudeTRUDF,yLatitudeTRUDF);
			if(rQSUDF.isContainPoint(xLongitudeUDF,yLatitudeUDF)){
				//多边形包含点
				ptr.set(new byte[getDataType().getByteSize()]);
				//返回1
				PInteger.INSTANCE.getCodec().encodeLong(1, ptr);
			} else {
				//多边形不包含点
				ptr.set(new byte[getDataType().getByteSize()]);
				//返回0
				PInteger.INSTANCE.getCodec().encodeLong(0, ptr);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}

	@Override
	public PDataType getDataType() {
		return PInteger.INSTANCE;
	}

	@Override
	public String getName() {
		return NAME;
	}
	
	
}
