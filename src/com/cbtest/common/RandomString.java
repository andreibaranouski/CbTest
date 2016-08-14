package com.cbtest.common;
import java.util.Random;

public class RandomString
{

  private static final char[] symbols = new char[36];

  static {
    for (int idx = 0; idx < 10; ++idx)
      symbols[idx] = (char) ('0' + idx);
    for (int idx = 10; idx < 36; ++idx)
      symbols[idx] = (char) ('a' + idx - 10);
  }

  private final static Random random = new Random();

  private static char[] buf;

  public static String getString(int length)
  {
	  if (buf == null || buf.length < length)
		  buf = new char[length];
	  for (int idx = 0; idx < length; ++idx) 
		  buf[idx] = symbols[random.nextInt(symbols.length)];
	  return new String(buf, 0, length);
  }

  public static String getName(int length)
  {
	  if (buf == null || buf.length < length)
		  buf = new char[length];
	  for (int idx = 0; idx < length; ++idx) 
		  buf[idx] = symbols[random.nextInt(symbols.length-10)+10];
	  return new String(buf, 0, length);
  }

   public static String getNumber(int length)
  {
	  if (buf == null || buf.length < length)
		  buf = new char[length];
	  for (int idx = 0; idx < length; ++idx) 
		  buf[idx] = symbols[random.nextInt(10)];
	  return new String(buf, 0, length);
  }

  public static synchronized String getHex(int length)
  {
	  if (buf == null || buf.length < length)
		  buf = new char[length];
	  for (int idx = 0; idx < length; ++idx) 
		  buf[idx] = symbols[random.nextInt(16)];
	  return new String(buf, 0, length);
  }
}