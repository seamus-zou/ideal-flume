package com.ideal.flume.source.iptv;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.lang3.StringUtils;

/**
 * Created by geeag on 17/6/26.
 */
public class IptvEncrypt {

    private Map<String, Set<Integer>> ruleMap = new HashMap<String, Set<Integer>>();

    public IptvEncrypt() {
        ruleMap.put("PRODUCT_ORDER", new HashSet<Integer>(0));  //﻿67163870|1000706391||SELFHELP|Sub|350|20170624 10:29:33|20650401 00:00:00|20170624 10:29:33
        ruleMap.put("TVOD_", new HashSet<Integer>(0, 4));    //﻿53104909|20170624 00:00:25|20170624 00:17:24|00000002000000070000000022627322|53104909@iptv2
        ruleMap.put("TV_", new HashSet<Integer>(0, 4));  //﻿48079077|20170619 23:01:58|20170624 01:56:58|00000002000000050000000000411431|48079077
        ruleMap.put("VOD_", new HashSet<Integer>(0, 4)); //﻿65319312|20170624 03:56:25|20170624 03:57:33|00000001000000010000000003043623|65319312@iptv2
        ruleMap.put("VAS_", new HashSet<Integer>(0, 4)); //
        ruleMap.put("KJ", new HashSet<Integer>(0, 1));   //﻿66902585|66902585@etv1|20170624 12:30:56
        ruleMap.put("USER", new HashSet<Integer>(0, 4));   //
    }

    public static String encrypt(IptvTypeEnum fileType, String line) {
        switch (fileType) {
		case PRODUCT_ORDER:
			return productOrderEncrypt(line);
			
		case VOD:
		case TV:
		case TVOD:
		case VAS:
			return vttvEncrypt(line);
			
		case KJ:
			return kjEncrypt(line);
			
		case USER:
		case ACTIVE_USER:
			return userEncrypt(line);
			
		default:
			return line;
		}
    }



    //PRODUCT_ORDER文件处理方法
    private static String productOrderEncrypt(String line) {
        String result = "";
        String[] info = line.split("\\|", -1);
        if(!"".equals(info[0]) && info[0] != null){
            info[0] = commonEncrypt(info[0]);
        }
        result = commCompose(info);
        return result;
    }

    //VOD、TV、TVOD、VAS文件处理方法
    private static String vttvEncrypt(String line){
        String result = "";
        String[] info = line.split("\\|", -1);
        if(!"".equals(info[0]) && info[0] != null){
            info[0] = commonEncrypt(info[0]);
        }
        if(!"".equals(info[4]) && info[4] != null){
            info[4] = particularCompose(info[4]);
        }
        result = commCompose(info);
        return result;
    }

    //KJ文件处理方法
    private static String kjEncrypt(String line){
        String result = "";
        String[] info = line.split("\\|", -1);
        if(!"".equals(info[0]) && info[0] != null){
            info[0] = commonEncrypt(info[0]);
        }
        if (!"".equals(info[1]) && info[1] != null) {
            info[1] = particularCompose(info[1]);
        }
        result = commCompose(info);
        return result;
    }


    //USER文件处理方法
    private static String userEncrypt(String line) {
        String[] info = line.split("\\|", -1);
        String result = "";
        try {
            if(!"".equals(info[0]) && info[0] != null){
                info[0] = encrypt("123", info[0]);
            }
            if (!"".equals(info[1]) && info[1] != null) {
                info[1] = commonEncrypt(info[1]);
            }
            if (!"".equals(info[2]) && info[2] != null) {
                info[2] = particularCompose(info[2]);
            }
            result = commCompose(info);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public static String encrypt(String seed, String cleartext) throws Exception {
        byte rawKey[] = getRawKey(seed.getBytes());
        SecretKeySpec skeySpec = new SecretKeySpec(rawKey, "AES");
        Cipher cipher = Cipher.getInstance("AES");
        cipher.init(1, skeySpec);
        byte encrypted[] = cipher.doFinal(cleartext.getBytes("gbk"));
        return toHex(encrypted);
    }

    public static String toHex(byte buf[]) {
        StringBuffer strbuf = new StringBuffer(buf.length * 2);
        for (int i = 0; i < buf.length; i++) {
            if ((buf[i] & 0xff) < 16)
                strbuf.append("0");
            strbuf.append(Long.toString(buf[i] & 0xff, 16));
        }
        return strbuf.toString();
    }

    private static byte[] getRawKey(byte seed[]) throws Exception {
        KeyGenerator kgen = KeyGenerator.getInstance("AES");
        SecureRandom sr = SecureRandom.getInstance("SHA1PRNG");
        sr.setSeed(seed);
        kgen.init(128, sr);
        SecretKey skey = kgen.generateKey();
        byte raw[] = skey.getEncoded();
        return raw;
    }

    private static String sha(String line) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA1");
            md.update(line.getBytes());
            byte[] hash = md.digest();
            StringBuilder builder = new StringBuilder();
            for (byte b : hash) {
                builder.append(Integer.toString((b & 0xff) + 0x100, 16).substring(1));
            }
            return builder.toString();
        } catch (NoSuchAlgorithmException nsae) {
            return null;
        }
    }

    private static String commonEncrypt(String line){
        if (line.length() == 8) {
            return sha("ad" + line);
        } else {
            return sha(line);
        }
    }

    private static String commCompose_org(String[] info){
        String result = "";
        for (String str : info) {
            if ("".equals(result)) {
                result = str;
            } else {
                result = result + "|" + str;
            }
        }
        return result + "\n";
    }
    
    private static String commCompose(String[] info){
        return StringUtils.join(info, '|');
    }

    private static String particularCompose(String info){
        String result = "";
        if (info.indexOf("@") >= 0) {
            String ad = info.substring(0, info.indexOf("@"));
            ad = commonEncrypt(ad);
            result = ad + info.substring(info.indexOf("@"));
        } else {
            result = commonEncrypt(info);
        }
        return result;
    }

    public static void main(String[] args) {
        String a = "dkvie@sq";
        String b = "jdjdksk";
        String c = "@jdjdksk";
        System.out.println(a.split("\\@", -1)[0]);
        System.out.println(a.substring(a.indexOf("@")));
        System.out.println(a.substring(0, a.indexOf("@")) + a.substring(a.indexOf("@")));

        String ss = "jdjdksk";
        if (!"".equals(ss) && ss != null) {
            if (ss.indexOf("@") >= 0) {
                String ad = ss.substring(0, ss.indexOf("@"));
                if (ad.length() == 8) {
                    ad = sha("ad" + ad);
                } else {
                    ad = sha(ad);
                }
                ss = ad + ss.substring(ss.indexOf("@"));
            } else {
                if (ss.length() == 8) {
                    ss = sha("ad" + ss);
                } else {
                    ss = sha(ss);
                }
            }
        }
        System.out.println("\n");
        System.out.println(ss);
    }
}
