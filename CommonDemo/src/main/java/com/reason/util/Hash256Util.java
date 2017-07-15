package com.reason.util;


import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Hash256Util {

	private static final String SHA_256 = "SHA-256";
	private static final String UTF_8 = "UTF-8";
	private static final String STRING_ZERO = "0";
	
	/**
	 * Hash the str to SHA_256
	 * @param str
	 * @return
	 * @throws NoSuchAlgorithmException
	 */
	public static String gethashCode(String str) {
		try {
			MessageDigest digest = MessageDigest.getInstance(SHA_256);
			byte[] strEncoded = str.getBytes(UTF_8);
			byte[] hash = digest.digest(strEncoded);
			int[] arr = new int[hash.length];

			for (int i = 0; i < hash.length; i++) {
				byte t = hash[i];
				int n = t < 0 ? t & 0xFF : t;
				arr[i] = n;
			}

			StringBuilder result = new StringBuilder();
			for (int b : arr) {
				result.append(FillStringToLength(Integer.toString(b, 16), STRING_ZERO, 2, true));
			}
			return result.toString();
		} catch(Exception e) {
			e.printStackTrace();
		}
		return null; 
	}

	private static String FillStringToLength(String sourceStr, String fillStr,
			int length, boolean frontFlag) {
		String tempStr = sourceStr;
		while (tempStr.length() < length) {
			if (frontFlag) {
				tempStr = fillStr + tempStr;
			} else {
				tempStr = tempStr + fillStr;
			}
		}

		if (tempStr.length() > length) {
			if (frontFlag) {
				tempStr = tempStr.substring(tempStr.length() - length, length);
			} else {
				tempStr = tempStr.substring(0, length);
			}
		}

		return tempStr;
	}
	public static void main(String[] args) {
		System.out.println(gethashCode("BGY4")+"|4621073");
		System.out.println(gethashCode("BGY4")+"|4621072");
	}
}
