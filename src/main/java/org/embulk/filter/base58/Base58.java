package org.embulk.filter.base58;

import java.math.BigInteger;

final class Base58
{
    private Base58()
    {
    }

    private static final String BASE_58_CHARS = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"; // Base-58 char library
    private static final BigInteger ZERO = new BigInteger("0");
    private static final BigInteger FIFTY_EIGHT = new BigInteger("58");

    public static String encode(String hex)
    {
        String originalHex = hex;

        BigInteger numeric = new BigInteger(hex, 16);
        String output = "";

        while (numeric.compareTo(ZERO) == 1) {
            BigInteger remainder = numeric.mod(FIFTY_EIGHT);
            numeric = numeric.divide(FIFTY_EIGHT);
            output = BASE_58_CHARS.charAt(Integer.parseInt(remainder.toString())) + output;
        }

        //leading zeros
        for (int i = 0; i < originalHex.length() && originalHex.substring(i, 2).equals("00"); i += 2) {
            output = "1" + output;
        }

        return output;
    }

    public static String decode(String base58Value)
    {
        String originalBase58 = base58Value;

        // Ignore bogus base58 strings
        if (base58Value.matches("[^1-9A-HJ-NP-Za-km-z]")) {
            return null;
        }

        BigInteger output = new BigInteger("0");

        for (int i = 0; i < base58Value.length(); i++) {
            int current = BASE_58_CHARS.indexOf(base58Value.charAt(i));
            output = output.multiply(FIFTY_EIGHT).add(new BigInteger(current + ""));
        }

        String hex = output.toString(16);

        // Leading zeros
        for (int ii = 0; ii < originalBase58.length() && originalBase58.charAt(ii) == '1'; ii++) {
            hex = "00" + hex;
        }

        if (hex.length() % 2 != 0) {
            hex = "0" + hex;
        }

        return hex.toLowerCase();
    }

    public static String encodeWithPrefix(String hex, String prefix)
    {
        return prefix + encode(hex);
    }

    public static String decodeWithPrefix(String baseValue, String prefix)
    {
        return decode(baseValue.replace(prefix, ""));
    }
}