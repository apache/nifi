package org.apache.nifi.processors.network.parser.util;

import java.math.BigInteger;
import java.util.Arrays;

public final class ConversionUtil {
	public static final BigInteger to_bigint(final byte[] buffer, final int offset, final int length) {
		return new BigInteger(Arrays.copyOfRange(buffer, offset, offset + length));
	}

	public static final byte to_byte(final byte[] buffer, final int offset) {
		return (byte) (buffer[offset] & 0xff);
	}

	public static final int to_int(final byte[] buffer, final int offset, final int length) {
		int ret = 0;
		final int done = offset + length;
		for (int i = offset; i < done; i++) {
			ret = ((ret << 8) & 0xffffffff) + (buffer[i] & 0xff);
		}
		return ret;
	}

	public static final long to_long(final byte[] buffer, final int offset, final int length) {
		long ret = 0;
		final int done = offset + length;
		for (int i = offset; i < done; i++) {
			ret = ret << 8;
			ret |= (buffer[i] & 0xFF);
		}
		return ret;
	}

	public static final short to_short(final byte[] buffer, final int offset, final int length) {
		short ret = 0;
		final int done = offset + length;
		for (int i = offset; i < done; i++) {
			ret = (short) (((ret << 8) & 0xffff) + (buffer[i] & 0xff));
		}
		return ret;
	}

	public static final String to_String(final byte[] buffer, final int offset, final int length) {
		return new String(Arrays.copyOfRange(buffer, offset, offset + length));
	}

	public static byte[] toByte(final int i) {
		final byte[] result = new byte[2];
		result[0] = (byte) (i >> 8);
		result[1] = (byte) (i);
		return result;
	}
}
