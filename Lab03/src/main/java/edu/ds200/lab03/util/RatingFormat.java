package edu.ds200.lab03.util;

import java.util.Locale;

public final class RatingFormat {
  private RatingFormat() {}

  public static String fourDecimals(double v) {
    return String.format(Locale.US, "%.4f", v);
  }
}
