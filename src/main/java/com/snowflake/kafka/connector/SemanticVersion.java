package com.snowflake.kafka.connector;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Represents a parsed semantic version. */
class SemanticVersion implements Comparable<SemanticVersion> {
  final int major;
  final int minor;
  final int patch;
  final boolean isReleaseCandidate;
  final String originalVersion;

  SemanticVersion(String version) {
    this.originalVersion = version;
    // Pattern to match versions like "3.1.0" or "4.0.0-rc" or "4.0.0-RC1"
    Pattern pattern = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)(?:-[rR][cC]\\d*)?");
    Matcher matcher = pattern.matcher(version);
    if (!matcher.find()) {
      throw new IllegalArgumentException("Invalid version format: " + version);
    }
    this.major = Integer.parseInt(matcher.group(1));
    this.minor = Integer.parseInt(matcher.group(2));
    this.patch = Integer.parseInt(matcher.group(3));
    this.isReleaseCandidate = version.toLowerCase().contains("-rc");
  }

  @Override
  public int compareTo(SemanticVersion other) {
    if (this.major != other.major) {
      return Integer.compare(this.major, other.major);
    }
    if (this.minor != other.minor) {
      return Integer.compare(this.minor, other.minor);
    }
    return Integer.compare(this.patch, other.patch);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof SemanticVersion)) {
      return false;
    }
    SemanticVersion other = (SemanticVersion) obj;
    return this.major == other.major && this.minor == other.minor && this.patch == other.patch;
  }

  @Override
  public int hashCode() {
    return Objects.hash(major, minor, patch);
  }

  @Override
  public String toString() {
    return originalVersion;
  }
}
