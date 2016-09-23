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


/**
 * UDFJTSDemo
 * 
 * @author QiuFeng
 *
 */
@BuiltInFunction(name = UDFRectangleDemoLWD.NAME, args = {@Argument(allowedTypes={PDouble.class, PDouble.class, PVarchar.class})})//
public class UDFRectangleDemoLWD extends ScalarFunction {
	
	public static final String NAME = "within";
	
	//Log log = LogFactory.getLog(UDFJTSDemo.class);
	public UDFRectangleDemoLWD() {
	}

	public UDFRectangleDemoLWD(List<Expression> children) {
		super(children);
	}
	
//	/**
//	 * 输入坐标，创建一个点 对象
//	 * @param x 横坐标
//	 * @param y 纵坐标
//	 * @return 返回一个节点对象
//	 * @throws ParseException 格式错误
//	 */
//	public Point createPoint(int x, int y) throws ParseException{
//		WKTReader reader = new WKTReader();
////		System.out.println("POINT("+x+" "+y+")");
//		Point point = (Point) reader.read("POINT("+x+" "+y+")");
//		return point;
//	}
//
//	/**
//	 * 输入多边形的点坐标，创建一个多边形的对象
//	 * @param str 多边形的各点坐标，必须遵循WKT的规则，收尾必须是同一个节点
//	 * @return 返回一个多边形
//	 * @throws ParseException 格式错误
//	 */
//	public Polygon createPolygon(String str) throws ParseException{
//		WKTReader reader = new WKTReader();
//		Polygon polygon = (Polygon) reader.read("POLYGON(("+str+"))");
//		return polygon;
//	}
//
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
			//创建点(x, y)实例
			//Point pt = createPoint(iarg1, iarg2);
			//创建对应的多边形实例
			//Polygon pg = createPolygon(sarg3);
			//判断点是否在多边形的范围内，此外类似的函数还有coveredBy、covers、within
			//但是在用within的时候，在UDF里，对于边界问题好像存在bug，所以在这里改用了contains
			//contains在处理边界是，边界上的点，不算多边形内部的点，返回false
//			if(pg.contains(pt)){
//				//多边形包含点
//				ptr.set(new byte[getDataType().getByteSize()]);
//				//返回1
//				PInteger.INSTANCE.getCodec().encodeLong(1, ptr);
//			} else {
//				//多边形不包含点
//				ptr.set(new byte[getDataType().getByteSize()]);
//				//返回0
//				PInteger.INSTANCE.getCodec().encodeLong(0, ptr);
//			}
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
