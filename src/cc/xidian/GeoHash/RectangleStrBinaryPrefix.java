package cc.xidian.GeoHash;

/**
 * Created by hadoop on 2016/9/10.
 */
public class RectangleStrBinaryPrefix {
    //public long prefix;//前缀码
    public String strBinaryPrefix;//字符串类型的前缀码
    public int length;//前缀码的位数，该位数为前缀码的二进制位数

    public RectangleStrBinaryPrefix(){
        this.strBinaryPrefix = "";
        this.length = 0;
    }

    public RectangleStrBinaryPrefix(String strBinaryPrefix, int length){
        this.strBinaryPrefix = strBinaryPrefix;
        this.length = length;
    }

    /**
     * 函数说明：前缀码最低位补1操作，得到新的子前缀码
     * @return 得到新的子前缀码
     */
    public RectangleStrBinaryPrefix attachOne() {
        //long bit = 0x8000000000000000L >>> length;//>>>为Java中的无符号位移，高位补0
        //String strBinaryGeoHashAttachOne = strBinaryPrefix+"1";
        return new RectangleStrBinaryPrefix(strBinaryPrefix+"1", length + 1);
    }
    /**
     * 函数说明：前缀码最低位补0操作，得到新的子前缀码
     * @return 新的子前缀码
     */
    public RectangleStrBinaryPrefix attachZero(){
        return new RectangleStrBinaryPrefix(strBinaryPrefix+"0",length+1);
    }
    public String toString(){
        return this.strBinaryPrefix+"#"+this.length;
    }
}
