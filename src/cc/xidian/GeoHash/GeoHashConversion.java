package cc.xidian.GeoHash;

import cc.xidian.GeoObject.RectangleQueryScope;

import java.awt.*;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Stack;

/**
 * Created by hadoop on 2016/5/20.
 */
public class GeoHashConversion {
    //共进行numBits*2次划分，geoHash的base32编码是numBits*2/5位，划分的格子数为（2的numBits次方*2的numBits次方）
    private static final int numBits = 6 * 5;
    private static final int SEARCH_DEPTH = 6*5;
    public static final int MAX_HASH_LENGTH = 12;
    private static final String BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz";
    public static final String BINARY_ZERO60 = "000000000000000000000000000000000000000000000000000000000000";//60个0的字符串
    public static final String BINARY_ONE60 = "111111111111111111111111111111111111111111111111111111111111";//60个1的字符串
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
     * 函数功能：将二进制字符串类型的geoHash值转换成double类型的经纬度坐标，改造时间：2016年9月22日11:48:37
     * @param strGeoHashBinary 二进制字符串类型的GeoHash值
     * @return double类型的经纬度坐标
     */
    public static double[] decodeLonAndLatFromGeoHashBinaryString(String strGeoHashBinary) {
//        String geoHash = base32(geoHashLong);//long类型的geoHash转换为String类型的Base32编码的GeoHash
//        StringBuilder buffer = new StringBuilder();
//        for (char c : geoHash.toCharArray()) {
//            int i = lookup.get(c) + 32;
//            buffer.append( Integer.toString(i, 2).substring(1) );
//        }

        BitSet lonSet = new BitSet();
        BitSet latSet = new BitSet();

        //even bits，偶数位处理，获得经度
        int j =0;
        for (int i=0; i< numBits*2;i+=2) {
            boolean isSet = false;
            if ( i < strGeoHashBinary.length() )
                isSet = strGeoHashBinary.charAt(i) == '1';
            lonSet.set(j++, isSet);
        }

        //odd bits，奇数位处理，获得纬度
        j=0;
        for (int i=1; i< numBits*2;i+=2) {
            boolean isSet = false;
            if ( i < strGeoHashBinary.length() )
                isSet = strGeoHashBinary.charAt(i) == '1';
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
     * 函数功能：将double类型的经纬度坐标转换成字符串类型的geoHash，经测试，编码正确，long类型的二进制编码是60位，这样才能保证得到12位的BASE32编码
     * @param lon double类型的经度坐标
     * @param lat double类型的纬度坐标
     * @return 返回字符串类型的geoHash值
     */
    public static long encodeGeoHashFromLonAndLatLong(double lon, double lat) {
        BitSet latBits = getBits(lat, -90, 90);
        BitSet lonBits = getBits(lon, -180, 180);
        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < numBits; i++) {
            buffer.append( (lonBits.get(i))?'1':'0');
            buffer.append( (latBits.get(i))?'1':'0');
        }
        return (Long.parseLong(buffer.toString(), 2));
    }
    /**
     * 函数功能：将double类型的经纬度坐标转换成二进制字符串类型的geoHash，经测试，编码正确，二进制编码是60位，改造时间：2016年9月22日11:46:57
     * @param lon double类型的经度坐标
     * @param lat double类型的纬度坐标
     * @return 返回二进制字符串类型的geoHash值
     */
    public static String encodeGeoHashFromLonAndLatBinaryString(double lon, double lat) {
        BitSet latBits = getBits(lat, -90, 90);
        BitSet lonBits = getBits(lon, -180, 180);
        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < numBits; i++) {
            buffer.append( (lonBits.get(i))?'1':'0');
            buffer.append( (latBits.get(i))?'1':'0');
        }
        return buffer.toString();
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
    public static String base32(long i) {
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
     * Takes a hash represented as a long and returns it as a string.
     *k
     * @param hash the hash, with the length encoded in the 4 least significant
     *             bits
     * @return the string encoded geohash
     */
    public static String fromLongToString(long hash) {
        int length = (int) (hash & 0xf);
        if (length > 12 || length < 1)
            throw new IllegalArgumentException("invalid long geohash " + hash);
        char[] geohash = new char[length];
        for (int pos = 0; pos < length; pos++) {
            geohash[pos] = BASE32.charAt(((int) (hash >>> 59)));
            hash <<= 5;
        }
        return new String(geohash);
    }

    /**
     * Returns a latitude,longitude pair as the centre of the given geohash.
     * Latitude will be between -90 and 90 and longitude between -180 and 180.
     *
     * @param geohash hash to decode
     * @return lat long point
     */
    // Translated to java from:
    // geohash.js
    // Geohash library for Javascript
    // (c) 2008 David Troy
    // Distributed under the MIT License

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
        //System.out.println(longLatBL[0]+"#"+longLatBL[1]);
        long maxGeoHashValueLong = (0xffffffffffffffffL >>> rP.length)+ rP.prefix;//根据最小前缀获得最大前缀
        //System.out.println(Long.toBinaryString(maxGeoHashValueLong));
        double[] longLatTR = HashToLongLat(maxGeoHashValueLong);//根据最大前缀获得右上角点的经纬度坐标
        return new RectangleQueryScope(longLatBL[0],longLatBL[1],longLatTR[0],longLatTR[1]);//根据左下角点和右上角点的经纬度坐标构造矩形查询范围，并返回
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

//        double[] xyBL = HashToLongLat(geoHashValueBL);
//        System.out.println(xyBL[0]+"#"+xyBL[1]);
        long geoHashValueTR = LongLatToHash(rQS.xLongitudeTR, rQS.yLatitudeTR);//获取矩形右上角点的GeoHash值，转码正确，但有精度误差
//        double[] xyTR = HashToLongLat(geoHashValueTR);
//        System.out.println(xyTR[0]+"#"+xyTR[1]);
        long rectanglePrefixXor = (geoHashValueBL ^ geoHashValueTR);//两个值异或，经测试，两个long类型的数异或后的值正确
//        System.out.println(Long.toBinaryString(geoHashValueBL));
//        System.out.println(Long.toBinaryString(geoHashValueTR));
//        System.out.println(Long.toBinaryString(rectanglePrefixXor));
        //计算前缀码的长度
        for(int i=0;i<numBits*2;i++){
            long temp = mask&rectanglePrefixXor;
            if(temp!=0){
                mask <<=1;
                break;
            }
            mask >>= 1;//带符号右移
            prefixLength++;
        }
//        System.out.println(Long.toBinaryString(geoHashValueBL&mask));
//        double[] geoBL = HashToLongLat(geoHashValueBL&mask);
//        System.out.println(geoBL[0]+"#"+geoBL[1]);
        return (new RectanglePrefix(geoHashValueBL&mask,prefixLength));
        //return rP;//构造前缀码并返回
    }

    /**
     * 函数功能：根据GeoHash索引进行范围查询，二叉树递归算法，经测试，该算法正确，该算法由张洋提供
     * @param rQS 矩形查询范围
     * @param searchDepthManual 手动设置搜索深度（暂定）
     * @return 查询结果的许多GeoHash范围值，long类型
     */
    public static ArrayList<long[]> rangeQueryWithGeoHashIndexAccordingToRectangleQueryScore(RectangleQueryScope rQS,int searchDepthManual){
        //搜索深度的判断
//        int searchDepth = SEARCH_DEPTH;
//        double rectangleMinWidth = (rQS.deltaX>=rQS.deltaY?rQS.deltaY:rQS.deltaX);
//        if(rectangleMinWidth<2){
//            searchDepth = SEARCH_DEPTH;
//        }else if(rectangleMinWidth<4){
//            searchDepth = 25;
//        }else if(rectangleMinWidth<8){
//            searchDepth = 20;
//        }else if(rectangleMinWidth<16){
//            searchDepth = 15;
//        }else{
//            searchDepth = 10;
//        }

        ArrayList<long[]> resultSet = new ArrayList<long[]>();
        RectanglePrefix rectanglePrefix = getRectanglePrefixFromRectangleQueryScope(rQS);//获取查询框的基本前缀
        Stack<RectanglePrefix> rPStack = new Stack<RectanglePrefix>();//使用栈进行递归操作
        rPStack.push(rectanglePrefix);//压栈
        //递归遍历二叉树操作
        while(!rPStack.empty()){
            RectanglePrefix rPNow = rPStack.pop();//从栈中弹出当前的矩形前缀码
            RectangleQueryScope rQSNow = getRectangleQueryScopeFromPrefix(rPNow);//根据前缀码计算出矩形范围
            //若查询范围包含当前矩形范围，则停止搜索，并将当前前缀码范围添加到最终结果集中
            if(rQS.isContainRectangleQueryScope(rQSNow)){
                long[] geoHashValueMinMax = new long[2];
                geoHashValueMinMax[0] = rPNow.prefix;
                geoHashValueMinMax[1] = (0xffffffffffffffffL>>>rPNow.length)+rPNow.prefix;
                resultSet.add(geoHashValueMinMax);
            }
            //若搜索深度没有达到最大深度，继续搜索
            else if(rPNow.length<searchDepthManual){
                RectanglePrefix attachOne = rPNow.attachOne();//补1操作
                RectanglePrefix attachZero = rPNow.attachZero();//补0操作
                RectangleQueryScope attachOneRectangle = getRectangleQueryScopeFromPrefix(attachOne);
                RectangleQueryScope attachZeroRectangle = getRectangleQueryScopeFromPrefix(attachZero);
                //若矩形范围相交，则将前缀码压栈
                if (rQS.isIntersectWithRectangleQueryScope(attachZeroRectangle)) {
                    rPStack.push(attachZero);
                }
                if(rQS.isIntersectWithRectangleQueryScope(attachOneRectangle)){
                    rPStack.push(attachOne);
                }
            }else{
                long[] geoHashValueMinMax = new long[2];
                geoHashValueMinMax[0] = rPNow.prefix;
                geoHashValueMinMax[1] = (0xffffffffffffffffL>>>rPNow.length)+rPNow.prefix;
                resultSet.add(geoHashValueMinMax);
            }
        }
        return resultSet;
    }




    //以下为60位二进制位的GeoHash编码方式测试，时间：2016年9月21日22:49:42，代码修改人：刘文东
    /**
     * 函数功能：从矩形查询范围得到矩形前缀码,前缀码为60位的二进制字符串，刘文东修订，经测试，转码正确，改造时间：2016年9月22日12:15:56
     * @param rQS 矩形查询范围
     * @return 矩形前缀码
     */
    public static RectangleStrBinaryPrefix getStrBinaryRectanglePrefixFromRectangleQueryScope(RectangleQueryScope rQS){
        String strBinaryGeoHashValueBL = encodeGeoHashFromLonAndLatBinaryString(rQS.xLongitudeBL,rQS.yLatitudeBL);
        String strBinaryGeoHashValueTR = encodeGeoHashFromLonAndLatBinaryString(rQS.xLongitudeTR,rQS.yLatitudeTR);
        long geoHashValueBL = Long.parseLong(strBinaryGeoHashValueBL,2);
        long geoHashValueTR = Long.parseLong(strBinaryGeoHashValueTR,2);
        long rectanglePrefixXor = (geoHashValueBL ^ geoHashValueTR);//两个值异或，60位，经测试，两个long类型的数异或后的值正确
        String strBinaryRectanglePrefixXor = Long.toBinaryString(rectanglePrefixXor);
        int prefixLength = strBinaryGeoHashValueBL.length() - strBinaryRectanglePrefixXor.length();//前缀码的长度
        String strBinaryPrefix = strBinaryGeoHashValueBL.substring(0,prefixLength);
        return (new RectangleStrBinaryPrefix(strBinaryPrefix,prefixLength));
    }
    /**
     * 函数功能：从前缀码中得到矩形范围,经测试，转码正确，刘文东修订，测试时间：2016-9-22 15:24:33
     * @param rSBP 矩形前缀码
     * @return 矩形查询范围
     */
    public static RectangleQueryScope getRectangleQueryScopeFromStrBinaryPrefix(RectangleStrBinaryPrefix rSBP){
        String strBinaryGeoHashBL = rSBP.strBinaryPrefix+BINARY_ZERO60.substring(0,(numBits*2-rSBP.length));
        String strBinaryGeoHashTR = rSBP.strBinaryPrefix+BINARY_ONE60.substring(0,(numBits*2-rSBP.length));
        double[] longLatBL = decodeLonAndLatFromGeoHashBinaryString(strBinaryGeoHashBL);//根据最小前缀获取左下角点的经纬度坐标
        double[] longLatTR = decodeLonAndLatFromGeoHashBinaryString(strBinaryGeoHashTR);//根据最大前缀获得右上角点的经纬度坐标
        return new RectangleQueryScope(longLatBL[0],longLatBL[1],longLatTR[0],longLatTR[1]);//根据左下角点和右上角点的经纬度坐标构造矩形查询范围，并返回
    }
    /**
     * 函数功能：根据GeoHash索引进行范围查询，二叉树递归算法，前缀码为60位的二进制形式，经测试，转码正确，刘文东修订，时间：2016年9月22日17:49:57
     * @param rQS 矩形查询范围
     * @param searchDepthManual 手动设置搜索深度（暂定）
     * @return 查询结果的许多GeoHash范围值，long类型
     */
    public static ArrayList<String[]> rangeQueryWithGeoHashIndexAccordingToRectangleQueryScore60(RectangleQueryScope rQS,int searchDepthManual){
        ArrayList<String[]> resultSet = new ArrayList<String[]>();
        RectangleStrBinaryPrefix rSBP = getStrBinaryRectanglePrefixFromRectangleQueryScope(rQS);
        Stack<RectangleStrBinaryPrefix> rSBPStack = new Stack<RectangleStrBinaryPrefix>();
        rSBPStack.push(rSBP);
        //递归遍历二叉树操作
        while(!rSBPStack.empty()){
            RectangleStrBinaryPrefix rSBPNow = rSBPStack.pop();//从栈中弹出当前的矩形前缀码
            RectangleQueryScope rQSNow = getRectangleQueryScopeFromStrBinaryPrefix(rSBPNow);//根据前缀码计算出矩形范围
            //若查询范围包含当前矩形范围，则停止搜索，并将当前前缀码范围添加到最终结果集中
            if(rQS.isContainRectangleQueryScope(rQSNow)){
                String[] geoHashValueMinMax = new String[2];
                geoHashValueMinMax[0] = rSBPNow.strBinaryPrefix+BINARY_ZERO60.substring(0,(numBits*2-rSBPNow.length));
                geoHashValueMinMax[1] = rSBPNow.strBinaryPrefix+BINARY_ONE60.substring(0,(numBits*2-rSBPNow.length));
                resultSet.add(geoHashValueMinMax);
            }
            //若搜索深度没有达到最大深度，继续搜索
            else if(rSBPNow.length<searchDepthManual){
                RectangleStrBinaryPrefix attachOne = rSBPNow.attachOne();//补1操作
                RectangleStrBinaryPrefix attachZero = rSBPNow.attachZero();//补0操作
                RectangleQueryScope attachOneRectangle = getRectangleQueryScopeFromStrBinaryPrefix(attachOne);
                RectangleQueryScope attachZeroRectangle = getRectangleQueryScopeFromStrBinaryPrefix(attachZero);
                //若矩形范围相交，则将前缀码压栈
                if (rQS.isIntersectWithRectangleQueryScope(attachZeroRectangle)) {
                    rSBPStack.push(attachZero);
                }
                if(rQS.isIntersectWithRectangleQueryScope(attachOneRectangle)){
                    rSBPStack.push(attachOne);
                }
            }else{
                String[] geoHashValueMinMax = new String[2];
                geoHashValueMinMax[0] = rSBPNow.strBinaryPrefix+BINARY_ZERO60.substring(0,(numBits*2-rSBPNow.length));
                geoHashValueMinMax[1] = rSBPNow.strBinaryPrefix+BINARY_ONE60.substring(0,(numBits*2-rSBPNow.length));
                resultSet.add(geoHashValueMinMax);
            }
        }
        return resultSet;
    }
}
