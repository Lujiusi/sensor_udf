package com.xinye.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import com.alibaba.fastjson.JSONObject;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author daiwei04@xinye.com
 * @date 2020/12/10 16:43
 * @desc sensor_udf as "com.xinye.udf.ActivityIdWithUserIdHash"
 */
public class ActivityIdWithUserIdHash extends UDF {

    private static final double DIVISOR = new BigInteger("18446744073709551616", 10).doubleValue();


    private static final JSONObject json = new JSONObject();

    public Text evaluate(Text activities, Long userId) throws NoSuchAlgorithmException {

        for (String s : activities.toString().split(",")) {
            json.put(s, getHashedProb(s + userId.toString()));
        }

        return new Text(json.toJSONString());
    }

    public static void main(String[] args) throws NoSuchAlgorithmException {
        ActivityIdWithUserIdHash activityIdWithUserIdHash = new ActivityIdWithUserIdHash();

        System.out.println(activityIdWithUserIdHash.evaluate(new Text("jojo,dd"), 1000L));
        System.out.println(activityIdWithUserIdHash.evaluate(new Text("jojo,dd"), 100L));
    }

    public double getHashedProb(String input) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[] messageDigest = md.digest(input.getBytes());
        byte[] firstUnsignedLong = new byte[8];
        for (int i = 0; i < 8; i++) {
            firstUnsignedLong[7 - i] = messageDigest[i];
        }
        BigInteger bigSignum = new BigInteger(1, firstUnsignedLong);
        return bigSignum.doubleValue() / DIVISOR;
    }

}
