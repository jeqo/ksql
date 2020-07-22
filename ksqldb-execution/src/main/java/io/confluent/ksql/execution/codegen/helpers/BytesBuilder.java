package io.confluent.ksql.execution.codegen.helpers;

public class BytesBuilder {

  private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

  private final String hex;

  public BytesBuilder(String hex) {
    this.hex = hex;
  }

  public byte[] build() {
    int len = hex.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
          + Character.digit(hex.charAt(i + 1), 16));
    }
    return data;
  }
}
