package cc.xidian.geoUtil;

import java.text.DecimalFormat;
import java.util.Random;

/**
 * Created by hadoop on 2016/9/8.
 */
public class RandomOperation {
    public static String RandomStringSimple(int length){
        String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random = new Random();
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int num = random.nextInt(62);
            buf.append(str.charAt(num));
        }
        return buf.toString();
    }
    public static double RandomDouble(double start,double end){
        Random r = new Random();
        DecimalFormat df = new DecimalFormat("#.00000");
        return Double.parseDouble(df.format(r.nextDouble()*(end - start)+start));
    }
}
