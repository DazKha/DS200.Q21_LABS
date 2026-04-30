package edu.ds200.lab03.util;

/** Nhóm tuổi thô (4 khoảng) — thường dùng trong lab DS200 cho phân tích theo độ tuổi. */
public final class AgeGrouping {
  private AgeGrouping() {}

  public static String bucket(int age) {
    if (age <= 18) {
      return "0-18";
    }
    if (age <= 35) {
      return "19-35";
    }
    if (age <= 50) {
      return "36-50";
    }
    return "51+";
  }
}
