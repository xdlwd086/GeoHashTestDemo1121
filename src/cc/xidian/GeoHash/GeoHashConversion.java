package cc.xidian.GeoHash;

import cc.xidian.GeoObject.GeoHashIndexRecord;
import cc.xidian.GeoObject.RectangleQueryScope;
import org.w3c.dom.css.Rect;

import java.awt.*;
import java.text.DecimalFormat;
import java.util.*;

/**
 * Created by hadoop on 2016/5/20.
 */
public class GeoHashConversion {
    //共进行numBits*2次划分，geoHash的base32编码是numBits*2/5位，划分的格子数为（2的numBits次方*2的numBits次方）
    public static final double LONSIZE = 360.0;
    public static final double LATSIZE = 180.0;
    public static final int numBits = 6 * 5;
    public static final int SEARCH_DEPTH_MAX = 60;
    public static final int MAX_HASH_LENGTH = 12;
    private static final String BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz";
    //BASE32编码
    final static char[] digits = { '0', '1', '2', '3', '4', '5', '6', '7', '8',
            '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n', 'p',
            'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z' };
    final static HashMap<Character, Integer> lookup = new HashMap<Character, Integer>();
    static {
        int i = 0;
        for (char c : digits)
            lookup.put(c, i++);
    }

    /**
     * 函数功能：将字符串类型的geoHash值转换成double类型的经纬度坐标，经测试，该函数转换正确
     * @param geoHash 字符串类型的GeoHash值
     * @return double类型的经纬度坐标
     */
    public static double[] decodeLonAndLatFromGeoHash(String geoHash) {

        StringBuilder buffer = new StringBuilder();
        for (char c : geoHash.toCharArray()) {
            int i = lookup.get(c) + 32;
            buffer.append( Integer.toString(i, 2).substring(1) );
        }
        BitSet lonSet = new BitSet();//经度数位
        BitSet latSet = new BitSet();//纬度数位
        //even bits 处理偶数位，纬度
        int j =0;
        for (int i=0; i< numBits*2;i+=2) {
            boolean isSet = false;
            if ( i < buffer.length() )
                isSet = buffer.charAt(i) == '1';
            lonSet.set(j++, isSet);
        }
        //odd bits 处理奇数位，经度
        j=0;
        for (int i=1; i< numBits*2;i+=2) {
            boolean isSet = false;
            if ( i < buffer.length() )
                isSet = buffer.charAt(i) == '1';
            latSet.set(j++, isSet);
        }

        double lon = decode(lonSet, -180, 180);
        double lat = decode(latSet, -90, 90);

        return new double[] {lon, lat};
    }
    /**
     * 函数功能：将long类型的geoHash值转换成double类型的经纬度坐标
     * @param geoHashLong 字符串类型的GeoHash值
     * @return double类型的经纬度坐标
     */
    public static double[] decodeLonAndLatFromGeoHashLong(long geoHashLong) {
        String geoHash = base32(geoHashLong);//long类型的geoHash转换为String类型的Base32编码的GeoHash
        StringBuilder buffer = new StringBuilder();
        for (char c : geoHash.toCharArray()) {
            int i = lookup.get(c) + 32;
            buffer.append( Integer.toString(i, 2).substring(1) );
        }

        BitSet lonSet = new BitSet();
        BitSet latSet = new BitSet();

        //even bits，偶数位处理，获得经度
        int j =0;
        for (int i=0; i< numBits*2;i+=2) {
            boolean isSet = false;
            if ( i < buffer.length() )
                isSet = buffer.charAt(i) == '1';
            lonSet.set(j++, isSet);
        }

        //odd bits，奇数位处理，获得纬度
        j=0;
        for (int i=1; i< numBits*2;i+=2) {
            boolean isSet = false;
            if ( i < buffer.length() )
                isSet = buffer.charAt(i) == '1';
            latSet.set(j++, isSet);
        }

        double lon = decode(lonSet, -180, 180);//经度
        double lat = decode(latSet, -90, 90);//纬度

        return new double[] {lon, lat};//经度在前，纬度在后
    }


    /**
     * 函数功能：
     * @param bs
     * @param floor
     * @param ceiling
     * @return
     */
    private static double decode(BitSet bs, double floor, double ceiling) {
        double mid = 0;
        for (int i=0; i<bs.length(); i++) {
            mid = (floor + ceiling) / 2;
            if (bs.get(i))
                floor = mid;
            else
                ceiling = mid;
        }
        return mid;
    }

    /**
     * 函数功能：将double类型的经纬度坐标转换成Base32编码类型的geoHash，经测试，该函数转码正确
     * @param lon double类型的经度坐标
     * @param lat double类型的纬度坐标
     * @return 返回字符串类型的geoHash值
     */
    public static String encodeGeoHashFromLonAndLat(double lon, double lat) {
        BitSet latBits = getBits(lat, -90, 90);
        BitSet lonBits = getBits(lon, -180, 180);
        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < numBits; i++) {
            buffer.append( (lonBits.get(i))?'1':'0');
            buffer.append( (latBits.get(i))?'1':'0');
        }
        return base32(Long.parseLong(buffer.toString(), 2));
    }
    /**
     * 函数功能：获取一个double的位集合
     * @param lat double类型的数
     * @param floor 最小值
     * @param ceiling 最大值
     * @return 返回位集合
     */
    private static BitSet getBits(double lat, double floor, double ceiling) {
        BitSet buffer = new BitSet(numBits);//创建一个大小为numBits的位集合，所有位初始化为false
        for (int i = 0; i < numBits; i++) {
            //利用二分法获取一个double的二进制序列
            double mid = (floor + ceiling) / 2;
            if (lat >= mid) {
                buffer.set(i);//若double值大于当前mid值，将改位置为true
                floor = mid;
            } else {
                ceiling = mid;
            }
        }
        return buffer;
    }

    /**
     * 函数功能：将一个long类型的数变成base32编码的字符串
     * @param i long类型的数
     * @return 返回base32编码的字符串
     */
    private static String base32(long i) {
        char[] buf = new char[65];
        int charPos = 64;
        boolean negative = (i < 0);
        if (!negative)
            i = -i;
        while (i <= -32) {
            buf[charPos--] = digits[(int) (-(i % 32))];
            i /= 32;
        }
        buf[charPos] = digits[(int) (-i)];
        if (negative)
            buf[--charPos] = '-';
        return new String(buf, charPos, (65 - charPos));
    }

    /**
     * 函数功能：double类型的经纬度坐标转long类型的GeoHash，经测试，转码正确，但long的编码二进制是64位，本函数由张洋提供
     * @param longitude 经度
     * @param latitude 纬度
     * @return long类型的GeoHash
     */
    public static long LongLatToHash(double longitude, double latitude) {
        boolean isEven = true;
        double minLat = -90.0, maxLat = 90.0;
        double minLon = -180.0, maxLon = 180.0;
        long bit = 0x8000000000000000L;
        long g = 0x0L;

        long target = 0x8000000000000000L >>> (5 * MAX_HASH_LENGTH);//无符号右移
        while (bit != target) {
            if (isEven) {
                double mid = (minLon + maxLon) / 2;
                if (longitude >= mid) {
                    g |= bit;
                    minLon = mid;
                } else
                    maxLon = mid;
            } else {
                double mid = (minLat + maxLat) / 2;
                if (latitude >= mid) {
                    g |= bit;
                    minLat = mid;
                } else
                    maxLat = mid;
            }

            isEven = !isEven;
            bit >>>= 1;
        }
        return g;
    }


    /**
     * 函数功能：long类型的GeoHash值转double类型的经纬度坐标，由张洋提供，经测试，转码正确
     * @param geoHash long类型的geoHash值
     * @return doule类型的经纬度坐标
     */
    public static double[] HashToLongLat(Long geoHash) {
        //to alternate longitude and latitude
        boolean isEven = true;
        double[] lat = new double[2];
        double[] lon = new double[2];
        lat[0] = -90.0;
        lat[1] = 90.0;
        lon[0] = -180.0;
        lon[1] = 180.0;
        //按位循环，逐渐逼近
        long bit = 0x8000000000000000L;
        for (int i = 0; i < 5 * MAX_HASH_LENGTH; i++) {
            boolean bitValue = (geoHash & bit) != 0;
            if (isEven) {
                refineInterval(lon, bitValue);
            } else {
                refineInterval(lat, bitValue);
            }
            isEven = !isEven;
            bit >>>= 1;
        }
        double resultLat = (lat[0] + lat[1]) / 2;
        double resultLon = (lon[0] + lon[1]) / 2;
        return new double[] {resultLon, resultLat};//经度在前，纬度在后
        //return new LongLat(resultLon, resultLat);
    }

    /**
     * Refines interval by a factor or 2 in either the 0 or 1 ordinate.
     *
     * @param interval two entry array of double values
     * @param bitValue
     */
    private static void refineInterval(double[] interval, boolean bitValue) {
        if (bitValue)
            interval[0] = (interval[0] + interval[1]) / 2;
        else
            interval[1] = (interval[0] + interval[1]) / 2;
    }
    /**
     * 函数功能：从前缀码中得到矩形范围，经测试，转码争取，该函数由张洋提供
     * @param rP 矩形前缀码，该前缀码为64位的long类型
     * @return 矩形查询范围
     */
    public static RectangleQueryScope getRectangleQueryScopeFromPrefix(RectanglePrefix rP){
        double[] longLatBL = HashToLongLat(rP.prefix);//根据最小前缀获取左下角点的经纬度坐标值
        long maxGeoHashValueLong = (0xffffffffffffffffL >>> rP.length)+ rP.prefix;//根据最小前缀获得最大前缀
        double[] longLatTR = HashToLongLat(maxGeoHashValueLong);//根据最大前缀获得右上角点的经纬度坐标
        return new RectangleQueryScope(longLatBL[0],longLatBL[1],longLatTR[0],longLatTR[1]);//根据左下角点和右上角点的经纬度坐标构造矩形查询范围，并返回
    }
    /**
     * 函数功能：从前缀码中得到矩形范围，经测试，转码争取，该函数由张洋提供
     * @param rP 矩形前缀码，该前缀码为64位的long类型
     * @return 矩形查询范围
     */
    public static long[] getGeoHashLongsFromPrefix(RectanglePrefix rP){
        long[] geoHashLongs = new long[2];
        geoHashLongs[0] = rP.prefix;
        geoHashLongs[1] = (0xffffffffffffffffL >>> rP.length)+ rP.prefix;
        return geoHashLongs;
    }
    /**
     * 函数功能：计算矩形框的面积，将矩形框的x增量和y增量分别取小数点后5位，然后乘以100000，变成整数，以提高乘法效率
     * @param rP 矩形前缀码，该前缀码为64位的long类型
     * @return long类型的乘法结果
     */
    public static long getRectangleQueryScopeAreaFromPrefix(RectanglePrefix rP){
        double[] longLatBL = HashToLongLat(rP.prefix);//根据最小前缀获取左下角点的经纬度坐标值
        long maxGeoHashValueLong = (0xffffffffffffffffL >>> rP.length)+ rP.prefix;//根据最小前缀获得最大前缀
        double[] longLatTR = HashToLongLat(maxGeoHashValueLong);//根据最大前缀获得右上角点的经纬度坐标
        DecimalFormat df = new DecimalFormat("#.00000");
        long deltaX = (long)(Double.parseDouble(df.format(Math.abs(longLatTR[0] - longLatBL[0])))*100000);
        long deltaY = (long)(Double.parseDouble(df.format(Math.abs(longLatTR[1] - longLatBL[1])))*100000);
        return deltaX*deltaY;
        //return new RectangleQueryScope(longLatBL[0],longLatBL[1],longLatTR[0],longLatTR[1]);//根据左下角点和右上角点的经纬度坐标构造矩形查询范围，并返回
    }

    /**
     * 函数功能：从矩形查询范围得到矩形前缀码，前缀码为64位的long类型
     * @param rQS 矩形查询范围
     * @return 矩形前缀码
     */
    public static RectanglePrefix getRectanglePrefixFromRectangleQueryScope(RectangleQueryScope rQS){
        int prefixLength = 0;//前缀码长度，初始化为0
        long mask = 0x8000000000000000L;//掩码，二进制位是64位
        long geoHashValueBL = LongLatToHash(rQS.xLongitudeBL, rQS.yLatitudeBL);//获取矩形左下角点的GeoHash值，转码正确，但有精度误差
//        System.out.println(geoHashValueBL);
//        System.out.println(Long.toBinaryString(geoHashValueBL));
        long geoHashValueTR = LongLatToHash(rQS.xLongitudeTR, rQS.yLatitudeTR);//获取矩形右上角点的GeoHash值，转码正确，但有精度误差
//        System.out.println(geoHashValueTR);
//        System.out.println(Long.toBinaryString(geoHashValueTR));
        long rectanglePrefixXor = (geoHashValueBL ^ geoHashValueTR);//两个值异或，经测试，两个long类型的数异或后的值正确
//        System.out.println(rectanglePrefixXor);
//        System.out.println(Long.toBinaryString(rectanglePrefixXor));
        //计算前缀码的长度
        for(int i=0;i<numBits*2;i++){
            long temp = mask&rectanglePrefixXor;
//            System.out.println(temp);
//            System.out.println(Long.toBinaryString(temp));
            if(temp!=0){
                mask <<=1;
                break;
            }
            mask >>= 1;//带符号右移
            prefixLength++;
        }
//        System.out.println(prefixLength);
//        System.out.println(mask);
//        System.out.println(Long.toBinaryString(mask));
        return (new RectanglePrefix(geoHashValueBL&mask,prefixLength));
    }

    /**
     * 函数功能：根据GeoHash索引进行范围查询，二叉树递归算法，并将相邻的GeoHash段合并
     * 算法再次修订：修正了一些错误，并将深度优先遍历方式改为广度优先遍历方式，递归结束标志改为面积比率，最后进行GeoHash段的合并
     * 编码人：刘文东，由刘文东进行重新设计与修订
     * 修订时间：2016年9月28日16:35:36
     * @param rQS 矩形查询范围
     * @param areaRatio 面积比率，需要通过实验进行测试，可暂时设为2.0
     * @return 查询结果的许多GeoHash范围值，long类型
     */
    public static Stack<long[]> getMergedGeoHashLongsByGeoHashIndexAlgorithmWithBFSAndAreaRatio(RectangleQueryScope rQS,double areaRatio){
        Stack<long[]> resultSet = new Stack<long[]>();//使用栈结构保存结果集
        //ArrayList<RectanglePrefix[]> rPArrayArray = new ArrayList<RectanglePrefix[]>();
        ArrayList<Stack<long[]>> resultSetArray = new ArrayList<Stack<long[]>>();
        //double rQSArea = Math.abs(rQS.deltaX)*Math.abs(rQS.deltaY);//计算查询区域的面积
        DecimalFormat df = new DecimalFormat("#.00000");
        long deltaX = (long)(Double.parseDouble(df.format(Math.abs(rQS.deltaX)))*100000);
        long deltaY = (long)(Double.parseDouble(df.format(Math.abs(rQS.deltaY)))*100000);
        double rQSArea = deltaX*deltaY;
        RectanglePrefix rectanglePrefix = getRectanglePrefixFromRectangleQueryScope(rQS);//获取查询框的基本前缀
        long rPArea = getRectangleQueryScopeAreaFromPrefix(rectanglePrefix);//
        ArrayDeque<RectanglePrefix> rPQueue = new ArrayDeque<RectanglePrefix>();//使用队列实现广度优先遍历
        rPQueue.push(rectanglePrefix);
        //int indexAA = 0;
        while(!rPQueue.isEmpty()){
            //保证队列中所有前缀长度相同，即处在同一遍历层，才能计算面积
            double rPQueueSizeStandard = Math.pow(2,(rPQueue.getFirst().length-rectanglePrefix.length));//计算各搜索深度的节点个数
            if(rPQueue.getLast().length == rPQueue.getFirst().length
                    &&(rPQueue.getFirst().length-rectanglePrefix.length)>0&&rPQueue.size()<rPQueueSizeStandard){
                //rPArrayArray.add((RectanglePrefix[])rPQueue.toArray());//将当前队列中所有元素添加到另外一个数组中
                //indexAA++;
                //计算当前队列中所有同层前缀对应面积的总和
                long rPQueueRectangleArea = rPArea>>>(rPQueue.getFirst().length-rectanglePrefix.length);
                long rPQueueRectangleAreaSum = 0;
                for(int i=0;i<rPQueue.size();i++){
                    rPQueueRectangleAreaSum += rPQueueRectangleArea;
                }
                //递归结束标志一：面积比较，若当前队列中留下的当前层的前缀对对应面积与查询范围面积的比值符合一定条件，则跳出循环，退出遍历
                if(rPQueueRectangleAreaSum/rQSArea>=1&&rPQueueRectangleAreaSum/rQSArea <= areaRatio){
                    break;
                }
            }
            //BFS，广度优先遍历，使用队列来实现
            RectanglePrefix rPNow = rPQueue.pollLast();
            //递归结束标志二，一般不会触发该标志，递归遍历主要靠标志一引起
            if(rPNow.length<(SEARCH_DEPTH_MAX + rectanglePrefix.length)){
                RectanglePrefix attachOne = rPNow.attachOne();//补1操作
                RectanglePrefix attachZero = rPNow.attachZero();//补0操作
                RectangleQueryScope attachOneRectangle = getRectangleQueryScopeFromPrefix(attachOne);
                RectangleQueryScope attachZeroRectangle = getRectangleQueryScopeFromPrefix(attachZero);
                //若矩形范围相交，则将前缀码压栈
                if (rQS.isIntersectWithRectangleQueryScope(attachZeroRectangle)) {
                    rPQueue.push(attachZero);
                }
                if(rQS.isIntersectWithRectangleQueryScope(attachOneRectangle)){
                    rPQueue.push(attachOne);
                }
            }
        }
        //遍历队列中的前缀，获得对应的geoHash段，进行合并操作
        for(RectanglePrefix rResult:rPQueue){
            long[] geoHashValueMinMax = new long[2];
            geoHashValueMinMax[0] = rResult.prefix;
            geoHashValueMinMax[1] = (0xffffffffffffffffL>>>rResult.length)+rResult.prefix;
            if(resultSet.empty()) {
                resultSet.push(geoHashValueMinMax);
            }else{
                long[] geoHashValueMinMaxPop = resultSet.pop();
                //相邻GeoHash段可以合并的条件：两个段组成的区域必须小于全球区域且相邻geoHash值相差为1
                if(geoHashValueMinMaxPop[0]!=0x8000000000000000L&&geoHashValueMinMaxPop[0]!=0xffffffffffffffffL
                        &&(geoHashValueMinMaxPop[0]-1)==geoHashValueMinMax[1]){
                    geoHashValueMinMax[1] = geoHashValueMinMaxPop[1];
                }else{
                    resultSet.push(geoHashValueMinMaxPop);//若不合并，则将弹出的元素重新放到结果栈中
                }
                resultSet.push(geoHashValueMinMax);
            }
        }
        return resultSet;
    }
    /**
     * 函数功能：根据GeoHash索引进行范围查询，二叉树递归算法，并将相邻的GeoHash段合并，该算法由张洋提
     * 算法再次修订：修正了一些错误，并将深度优先遍历方式改为广度优先遍历方式，递归结束标志改为面积比率，最后进行GeoHash段的合并
     * 编码人：刘文东，由刘文东进行重新设计与修订
     * 修订时间：2016年9月28日16:35:36
     * @param rQS 矩形查询范围
     * @return 查询结果的许多GeoHash范围值，long类型
     */
    public static Stack<GeoHashIndexRecord> getMergedGeoHashLongsByGeoHashIndexAlgorithmWithBFSAndAreaRatioLWD(RectangleQueryScope rQS){
        //Stack<long[]> resultSet = new Stack<long[]>();//使用栈结构保存结果集
        //ArrayList<ArrayList<RectanglePrefix>> rPArrayArray = new ArrayList<ArrayList<RectanglePrefix>>();
        //ArrayList<Stack<long[]>> resultSetArray = new ArrayList<Stack<long[]>>();
        //ArrayList<GeoHashIndexRecord> gHIRArray = new ArrayList<GeoHashIndexRecord>();
        int levelLon = (int)(Math.log(LONSIZE/rQS.deltaX)/Math.log(2));
        int levelLat = (int)(Math.log(LATSIZE/rQS.deltaY)/Math.log(2));
        int levelMax = Math.max(levelLon,levelLat);
        Stack<GeoHashIndexRecord> gHIRStack = new Stack<GeoHashIndexRecord>();
        int[] bfsLayerSize = new int[50];
        bfsLayerSize[0] = 1;
        //计算查询区域的面积，扩大10的10次方倍
        DecimalFormat df = new DecimalFormat("#.00000");
        long deltaX = (long)(Double.parseDouble(df.format(Math.abs(rQS.deltaX)))*100000);
        long deltaY = (long)(Double.parseDouble(df.format(Math.abs(rQS.deltaY)))*100000);
        double rQSArea = deltaX*deltaY;//计算查询范围的面积
        RectanglePrefix rectanglePrefix = getRectanglePrefixFromRectangleQueryScope(rQS);//获取查询框的基本前缀
        long rPArea = getRectangleQueryScopeAreaFromPrefix(rectanglePrefix);//由基本前缀获取基本矩形的面积
        ArrayDeque<RectanglePrefix> rPQueue = new ArrayDeque<RectanglePrefix>();//使用队列实现广度优先遍历
        rPQueue.push(rectanglePrefix);//将基本前缀压栈
        //递归遍历二叉树操作，深度优先遍历，DFS
        //int indexAAR = 0;
        while(!rPQueue.isEmpty()){
            //保证队列中所有前缀长度相同，即处在同一遍历层，才能计算面积
            double rPQueueSizeStandard = Math.pow(2,(rPQueue.getFirst().length-rectanglePrefix.length));//计算各搜索深度的节点个数
//            double rPQueueSizeStandard = Math.pow(2,(rPQueue.getFirst().length-rectanglePrefix.length));//计算各搜索深度的节点个数
            //如果队列中的节点刚好处于同一层且搜索深度大于0且队列中元素的个数小于该层节点的标准个数（即该层节点有舍弃的节点）
//            if(rPQueue.getLast().length == rPQueue.getFirst().length
//                    &&(rPQueue.getFirst().length-rectanglePrefix.length)>0&&rPQueue.size()<rPQueueSizeStandard){
            if(rPQueue.getLast().length == rPQueue.getFirst().length &&(rPQueue.getFirst().length-rectanglePrefix.length)>0){
                bfsLayerSize[rPQueue.getFirst().length-rectanglePrefix.length] = rPQueue.size();//保存当前层的元素个数
                //如果当前层有元素被舍弃
                if(rPQueue.size()<bfsLayerSize[rPQueue.getFirst().length-rectanglePrefix.length-1]*2) {
                    //开始构造索引记录对象
                    GeoHashIndexRecord g = new GeoHashIndexRecord();
                    g.searchDepth = rPQueue.getFirst().length - rectanglePrefix.length;//搜索深度
//                    System.out.println(g.searchDepth+"S");
                    g.rQS = rQS;//查询范围
                    g.sizeOfRectanglePrefix = rPQueue.size();//当前层前缀码对象个数
//                    System.out.println(g.sizeOfRectanglePrefix);
                    //计算当前队列中所有同层前缀对应面积的总和
                    long rPQueueRectangleArea = rPArea >>> (rPQueue.getFirst().length - rectanglePrefix.length);//当前层前缀码对于的矩形面积
                    long rPQueueRectangleAreaSum = 0;//面积和
                    for (RectanglePrefix r : rPQueue) {
//                        RectangleQueryScope rQSQueue = getRectangleQueryScopeFromPrefix(r);
//                        System.out.println(r.length+"L");
//                        System.out.println(Long.toBinaryString(r.prefix));
//                        System.out.println(rQSQueue.toString());
                        rPQueueRectangleAreaSum += rPQueueRectangleArea;//求面积和
                        g.rPArray.add(r);//将当前层的所有前缀码添加到索引记录对象的前缀码数组中
                    }
                    //将前缀码数组中的前缀码取出，求对应的GeoHash段，进行合操作，并将合并后的GeoHash段添加到索引记录对象的GeoHash段数组中
                    g.getMergedGeoHashLongsFromRectanglePrefixArray();
                    g.sizeOfGeoHashLongs = g.sGeoHashLongs.size();//求合并后的GeoHash段的个数

                    double areaRatioNow = rPQueueRectangleAreaSum / rQSArea;//面积比例的计算
                    g.areaRatio = areaRatioNow;//记录面积比率
                    //递归结束标志一：面积比较，若当前队列中留下的当前层的前缀对对应面积与查询范围面积的比值小于1，则跳出循环，退出遍历
                    if (areaRatioNow <= 1.1) {
                        break;
                    }
                    //若合并后的GeoHash段相同，则选择搜索深度最深的结果
                    if (gHIRStack.empty()) {
                        gHIRStack.push(g);
                    } else {
                        GeoHashIndexRecord gPop = gHIRStack.pop();
                        if (gPop.sizeOfGeoHashLongs == g.sizeOfGeoHashLongs) {
                            gHIRStack.push(g);
                        } else {
                            gHIRStack.push(gPop);
                            gHIRStack.push(g);
                        }
                    }
                }
            }
            //BFS，广度优先遍历，使用队列来实现
            RectanglePrefix rPNow = rPQueue.pollLast();
            //递归结束标志二，一般不会触发该标志，递归遍历主要靠标志一引起
            if(rPNow.length<numBits*2){
//                System.out.println(rPNow.length+"L");
                RectanglePrefix attachOne = rPNow.attachOne();//补1操作
                RectanglePrefix attachZero = rPNow.attachZero();//补0操作
                RectangleQueryScope attachOneRectangle = getRectangleQueryScopeFromPrefix(attachOne);
                RectangleQueryScope attachZeroRectangle = getRectangleQueryScopeFromPrefix(attachZero);
                //若矩形范围相交，则将前缀码压栈
                if(rQS.isIntersectWithRectangleQueryScope(attachZeroRectangle)) {
                    rPQueue.push(attachZero);
                }
                if(rQS.isIntersectWithRectangleQueryScope(attachOneRectangle)){
                    rPQueue.push(attachOne);
                }
            }
        }
        return gHIRStack;
    }
    /**
     * 函数功能：根据GeoHash索引进行范围查询，二叉树递归算法，并将相邻的GeoHash段合并，该算法由张洋提
     * 算法再次修订：修正了一些错误，并将深度优先遍历方式改为广度优先遍历方式，递归结束标志改为面积比率，最后进行GeoHash段的合并
     * 编码人：刘文东，由刘文东进行重新设计与修订
     * 修订时间：2016年9月28日16:35:36
     * @param rQS 矩形查询范围
     * @return 查询结果的许多GeoHash范围值，long类型
     */
    public static Stack<GeoHashIndexRecord> getMergedGeoHashLongsByGeoHashIndexAlgorithmWithBFSAndAreaRatioLWDTest(RectangleQueryScope rQS) {
        //Stack<long[]> resultSet = new Stack<long[]>();//使用栈结构保存结果集
        //ArrayList<ArrayList<RectanglePrefix>> rPArrayArray = new ArrayList<ArrayList<RectanglePrefix>>();
        //ArrayList<Stack<long[]>> resultSetArray = new ArrayList<Stack<long[]>>();
        //ArrayList<GeoHashIndexRecord> gHIRArray = new ArrayList<GeoHashIndexRecord>();
        int levelLon = (int) (Math.log(LONSIZE / rQS.deltaX) / Math.log(2));
        int levelLat = (int) (Math.log(LATSIZE / rQS.deltaY) / Math.log(2));
        int levelMax = Math.max(levelLon, levelLat);
        System.out.println(levelMax);
        Stack<GeoHashIndexRecord> gHIRStack = new Stack<GeoHashIndexRecord>();
        int[] bfsLayerSize = new int[50];
        bfsLayerSize[0] = 1;
        //计算查询区域的面积，扩大10的10次方倍
        DecimalFormat df = new DecimalFormat("#.00000");
        long deltaX = (long) (Double.parseDouble(df.format(Math.abs(rQS.deltaX))) * 100000);
        long deltaY = (long) (Double.parseDouble(df.format(Math.abs(rQS.deltaY))) * 100000);
        double rQSArea = deltaX * deltaY;//计算查询范围的面积
        RectanglePrefix rectanglePrefix = getRectanglePrefixFromRectangleQueryScope(rQS);//获取查询框的基本前缀
        long rPArea = getRectangleQueryScopeAreaFromPrefix(rectanglePrefix);//由基本前缀获取基本矩形的面积
        ArrayDeque<RectanglePrefix> rPQueue = new ArrayDeque<RectanglePrefix>();//使用队列实现广度优先遍历
        rPQueue.push(rectanglePrefix);//将基本前缀压栈
        //递归遍历二叉树操作，深度优先遍历，DFS
        //int indexAAR = 0;
        while (!rPQueue.isEmpty()) {
            //保证队列中所有前缀长度相同，即处在同一遍历层，才能计算面积
//            double rPQueueSizeStandard = Math.pow(2, (rPQueue.getFirst().length - rectanglePrefix.length));//计算各搜索深度的节点个数
//            double rPQueueSizeStandard = Math.pow(2,(rPQueue.getFirst().length-rectanglePrefix.length));//计算各搜索深度的节点个数
            //如果队列中的节点刚好处于同一层且搜索深度大于0且队列中元素的个数小于该层节点的标准个数（即该层节点有舍弃的节点）
//            if(rPQueue.getLast().length == rPQueue.getFirst().length
//                    &&(rPQueue.getFirst().length-rectanglePrefix.length)>0&&rPQueue.size()<rPQueueSizeStandard){
            if (rPQueue.getLast().length == rPQueue.getFirst().length && (rPQueue.getFirst().length - rectanglePrefix.length) > 0) {
                bfsLayerSize[rPQueue.getFirst().length - rectanglePrefix.length] = rPQueue.size();//保存当前层的元素个数
                //如果当前层有元素被舍弃
                if (rPQueue.size() < bfsLayerSize[rPQueue.getFirst().length - rectanglePrefix.length - 1] * 2) {
                    //开始构造索引记录对象
                    GeoHashIndexRecord g = new GeoHashIndexRecord();
                    g.searchDepth = rPQueue.getFirst().length - rectanglePrefix.length;//搜索深度
//                    System.out.println(g.searchDepth+"S");
                    g.rQS = rQS;//查询范围
                    g.sizeOfRectanglePrefix = rPQueue.size();//当前层前缀码对象个数
//                    System.out.println(g.sizeOfRectanglePrefix);
                    //计算当前队列中所有同层前缀对应面积的总和
                    long rPQueueRectangleArea = rPArea >>> (rPQueue.getFirst().length - rectanglePrefix.length);//当前层前缀码对于的矩形面积
                    long rPQueueRectangleAreaSum = 0;//面积和
                    for (RectanglePrefix r : rPQueue) {
                        RectangleQueryScope rQSQueue = getRectangleQueryScopeFromPrefix(r);
//                        System.out.println(r.length+"L");
//                        System.out.println(Long.toBinaryString(r.prefix));
//                        System.out.println(rQSQueue.toString());
                        rPQueueRectangleAreaSum += rPQueueRectangleArea;//求面积和
                        g.rPArray.add(r);//将当前层的所有前缀码添加到索引记录对象的前缀码数组中
                    }
                    //将前缀码数组中的前缀码取出，求对应的GeoHash段，进行合操作，并将合并后的GeoHash段添加到索引记录对象的GeoHash段数组中
                    g.getMergedGeoHashLongsFromRectanglePrefixArray();
                    g.sizeOfGeoHashLongs = g.sGeoHashLongs.size();//求合并后的GeoHash段的个数

                    double areaRatioNow = rPQueueRectangleAreaSum / rQSArea;//面积比例的计算
                    g.areaRatio = areaRatioNow;//记录面积比率
                    //递归结束标志一：面积比较，若当前队列中留下的当前层的前缀对对应面积与查询范围面积的比值小于1，则跳出循环，退出遍历
//                    if (areaRatioNow <= 1.1) {
//                        break;
//                    }

                    //若合并后的GeoHash段相同，则选择搜索深度最深的结果
                    if (gHIRStack.empty()) {
                        gHIRStack.push(g);
                    } else {
                        GeoHashIndexRecord gPop = gHIRStack.pop();
                        if (gPop.sizeOfGeoHashLongs == g.sizeOfGeoHashLongs) {
                            gHIRStack.push(g);
                        } else {
                            gHIRStack.push(gPop);
                            gHIRStack.push(g);
                        }
                    }
                }

            }
        //BFS，广度优先遍历，使用队列来实现
            if (rPQueue.getFirst().length == levelMax * 2) {
                break;
            }
                RectanglePrefix rPNow = rPQueue.pollLast();
            //递归结束标志二，一般不会触发该标志，递归遍历主要靠标志一引起
            if (rPNow.length < (levelMax * 2)) {
//                System.out.println(rPNow.length+"L");
                RectanglePrefix attachOne = rPNow.attachOne();//补1操作
                RectanglePrefix attachZero = rPNow.attachZero();//补0操作
                RectangleQueryScope attachOneRectangle = getRectangleQueryScopeFromPrefix(attachOne);
                RectangleQueryScope attachZeroRectangle = getRectangleQueryScopeFromPrefix(attachZero);
                //若矩形范围相交，则将前缀码压栈
                if (rQS.isIntersectWithRectangleQueryScope(attachZeroRectangle)) {
                    rPQueue.push(attachZero);
                }
                if (rQS.isIntersectWithRectangleQueryScope(attachOneRectangle)) {
                    rPQueue.push(attachOne);
                }
//            } else if (rPNow.length == (levelMax * 2)) {
//                rPQueue.push(rPNow);
//            }
            }
        }
        return gHIRStack;
    }


    public static void getMergedGeoHashLongsByGeoHashIndexAlgorithmWithCenterLWD(RectangleQueryScope rQS){

        long geoHashValueBL = LongLatToHash(rQS.xLongitudeBL, rQS.yLatitudeBL);//获取矩形左下角点的GeoHash值，转码正确，但有精度误差
        String strGeoHashValueBL = encodeGeoHashFromLonAndLat(rQS.xLongitudeBL,rQS.yLatitudeBL);
        System.out.println(strGeoHashValueBL);
        System.out.println(geoHashValueBL);
        System.out.println(Long.toBinaryString(geoHashValueBL));
        long geoHashValueTR = LongLatToHash(rQS.xLongitudeTR, rQS.yLatitudeTR);//获取矩形右上角点的GeoHash值，转码正确，但有精度误差
        String strGeoHashValueTR = encodeGeoHashFromLonAndLat(rQS.xLongitudeTR,rQS.yLatitudeTR);
        System.out.println(strGeoHashValueTR);
        System.out.println(geoHashValueTR);
        System.out.println(Long.toBinaryString(geoHashValueTR));
        long geoHashValueCenter = LongLatToHash(rQS.xLongitudeCenter,rQS.yLatitudeCenter);
        String strGeoHashValueCenter = encodeGeoHashFromLonAndLat(rQS.xLongitudeCenter,rQS.yLatitudeCenter);
        System.out.println(strGeoHashValueCenter);
        System.out.println(rQS.toString());
        System.out.println(rQS.xLongitudeCenter+","+rQS.yLatitudeCenter);
        System.out.println(geoHashValueCenter);
        System.out.println(Long.toBinaryString(geoHashValueCenter));
        System.out.println(rQS.deltaX+" "+rQS.deltaY);
        long maskB = 0xFL;
        long maskA = 0xFFFFFFFFFFFFFFF0L;
        int levelLon = (int)(Math.log(LONSIZE/rQS.deltaX)/Math.log(2));
        int levelLat = (int)(Math.log(LATSIZE/rQS.deltaY)/Math.log(2));
        int levelMax = Math.max(levelLon,levelLat);
        int lengthCode = (int)Math.floor(0.4*levelMax);
        System.out.println((Math.log(LONSIZE/rQS.deltaX)/Math.log(2)));
        System.out.println((Math.log(LATSIZE/rQS.deltaY)/Math.log(2)));
        System.out.println(levelMax);
        System.out.println(lengthCode);
        for(int len=lengthCode;len>0;len--){
            String strGeoHashValueCenterCan = strGeoHashValueCenter.substring(0,len);

            System.out.println(strGeoHashValueCenterCan);
        }
//        RectanglePrefix rectanglePrefix = getRectanglePrefixFromRectangleQueryScope(rQS);
//        System.out.println(rectanglePrefix.toString());
//        for(int i=60;i>=0;i--){
//            long tempA = maskA&geoHashValueCenter;
//            double[] xyA = HashToLongLat(tempA);
//            long tempB = maskB|geoHashValueCenter;
//            double[] xyB = HashToLongLat(tempB);
//            maskA<<=1;
//            maskB<<=1;
//            maskB++;
//
//            System.out.println(i);
//            System.out.println(Long.toBinaryString(tempA));
//            System.out.println(xyA[0]+","+xyA[1]);
//            System.out.println(Long.toBinaryString(tempB));
//            System.out.println(xyB[0]+","+xyB[1]);
//            System.out.println(Long.toBinaryString(maskA));
//            System.out.println(Long.toBinaryString(maskB));
//            RectangleQueryScope rQSCenter = new RectangleQueryScope(xyA[0],xyA[1],xyB[0],xyB[1]);
//            if(rQSCenter.isContainRectangleQueryScope(rQS)){
//                break;
//            }

//        }



    }
}
