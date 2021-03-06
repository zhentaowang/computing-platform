package com.adatafun.computing.platform.util;

import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.SecretKeySpec;
import javax.crypto.spec.IvParameterSpec;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * desc :aes加密工具
 * Created by Lin on 2017/8/23.
 */
public class AesUtil {
    private IvParameterSpec ivSpec;
    private SecretKeySpec keySpec;

    public AesUtil(String key) {
        try {
            byte[] keyBytes = key.getBytes();
            byte[] buf = new byte[16];

            for (int i = 0; i < keyBytes.length && i < buf.length; i++) {
                buf[i] = keyBytes[i];
            }

            this.keySpec = new SecretKeySpec(buf, "AES");
            this.ivSpec = new IvParameterSpec(keyBytes);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String encrypt(String origData) {
        try {
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE, this.keySpec, this.ivSpec);
            return base64Encode(cipher.doFinal(origData.getBytes()));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public String decrypt(String crypted) {
        try {
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            cipher.init(Cipher.DECRYPT_MODE, this.keySpec, this.ivSpec);
            byte[] decode = base64Decode(crypted);
            if (decode.length%16 == 0) {
                return new String(cipher.doFinal(base64Decode(crypted)));
            } else {
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        AesUtil aes = new AesUtil("fengshu_20170228");
        String data = "340621199309201234";
        String crypted = aes.encrypt(data);
        System.out.println(crypted);
        System.out.println(aes.decrypt("p4OKa8n6stgpo6I2f8k/3Q=="));
        System.out.println("bg/e/xzm3A97fJj0MaEERA==".length());
        Map<String, String> map = new HashMap<>();
        map.put("1", "00");
        map.put("2", "00");
        System.out.println(map);
        System.out.println(map.toString());
    }

    public static String base64Encode(byte[] data) {
        BASE64Encoder encoder = new BASE64Encoder();
        return encoder.encode(data);
    }

    public static byte[] base64Decode(String data) {
        if (null == data) return null;
        BASE64Decoder decoder = new BASE64Decoder();
        try {
            return decoder.decodeBuffer(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
