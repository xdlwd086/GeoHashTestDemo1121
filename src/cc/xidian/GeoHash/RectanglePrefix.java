package cc.xidian.GeoHash;

/**
 * Created by hadoop on 2016/9/10.
 */
public class RectanglePrefix {
    public long prefix;//前缀码
    //public String strPrefix;//字符串类型的前缀码
    public int length;//前缀码的位数，该位数为前缀码的二进制位数

    public RectanglePrefix(){
        this.prefix = 0;
        this.length = 0;
    }

    public RectanglePrefix(long prefix,int length){
        this.prefix = prefix;
        this.length = length;
    }

    /**
     * 函数说明：前缀码最低位补1操作，得到新的子前缀码
     * @return 得到新的子前缀码
     */
    public RectanglePrefix attachOne() {
        long bit = 0x8000000000000000L >>> length;//>>>为Java中的无符号位移，高位补0
        return new RectanglePrefix(prefix | bit, length + 1);
    }
    /**
     * 函数说明：前缀码最低位补0操作，得到新的子前缀码
     * @return 新的子前缀码
     */
    public RectanglePrefix attachZero(){
        return new RectanglePrefix(prefix,length+1);
    }
    public String toString(){
        return this.prefix+"#"+this.length;
    }
}
