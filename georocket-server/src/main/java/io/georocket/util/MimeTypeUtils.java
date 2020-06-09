package io.georocket.util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.input.BOMInputStream;

/**
 * Utility methods for mime types
 * @author Andrej Sajenko
 * @author Michel Kraemer
 */
public class MimeTypeUtils {
  /**
   * Mime type for XML
   */
  public static final String XML = "application/xml";

  /**
   * Mime type for JSON
   */
  public static final String JSON = "application/json";

  /**
   * Mime type for LAS (LASer File Format)
   * This includes the compressed format LAZ.
   * https://www.iana.org/assignments/media-types/application/vnd.las
   */
  public static final String LAS = "application/vnd.las";

  /**
   * <p>Check if the given mime type belongs to another one.</p>
   * <p>Examples:</p>
   * <ul>
   *   <li>belongsTo("application/gml+xml", "application", "xml") == true</li>
   *   <li>belongsTo("application/exp+xml", "application", "xml") == true</li>
   *   <li>belongsTo("application/xml", "application", "xml") == true</li>
   *   <li>belongsTo("application/exp+xml", "text", "xml") == false</li>
   *   <li>belongsTo("application/exp+xml", "application", "json") == false</li>
   * </ul>
   * @param mimeType the mime type
   * @param otherType the general type of the other mime type
   * @param otherStructuredSyntaxSuffix the structured syntax suffix of the
   * other subtype (subtype = example+structuredSyntaxSuffix)
   * @return true if the mime type belongs to the other one
   */
  public static boolean belongsTo(String mimeType, String otherType,
      String otherStructuredSyntaxSuffix) {
    String mediaParts[] = mimeType.split("/");
    if (mediaParts.length != 2) {
      return false;
    }

    String type = mediaParts[0];
    String subtype = mediaParts[1];

    if (!type.equals(otherType)) {
      return false;
    }

    if (subtype.equals(otherStructuredSyntaxSuffix)) {
      return true;
    }

    String subtypeParts[] = subtype.split("\\+");
    if (subtypeParts.length != 2) {
      return false;
    }

    String structuredSyntaxSuffix = subtypeParts[1];
    return structuredSyntaxSuffix.equals(otherStructuredSyntaxSuffix);
  }

  /**
   * Read the first bytes of the given file and try to determine the file
   * format. Read up to 100 KB before giving up.
   * @param f the file to read
   * @return the file format (or <code>null</code> if the format
   * could not be determined)
   * @throws IOException if the input stream could not be read
   */
  public static String detect(File f) throws IOException {
    return detect(f, false);
  }

  /**
   * Read the first bytes of the given file and try to determine the file
   * format. Read up to 100 KB before giving up.
   * @param f the file to read
   * @param gzip true if the file is compressed with GZIP
   * @return the file format (or <code>null</code> if the format
   * could not be determined)
   * @throws IOException if the input stream could not be read
   */
  public static String detect(File f, boolean gzip) throws IOException {
    if (!f.exists()) {
      return null;
    }
    InputStream is = null;
    try {
      is = new FileInputStream(f);
      if (gzip) {
        is = new GZIPInputStream(is);
      }
      try (BufferedInputStream bis = new BufferedInputStream(new BOMInputStream(is))) {
        return determineFileFormat(bis);
      }
    } finally {
      if (is != null) {
        is.close();
      }
    }
  }

  /**
   * Read the first bytes of the given input stream and try to
   * determine the file format. Reset the input stream to the position
   * it had when the method was called. Read up to 100 KB before
   * giving up.
   * @param bis a buffered input stream that supports the mark and reset
   * methods
   * @return the file format (or <code>null</code> if the format
   * could not be determined)
   * @throws IOException if the input stream could not be read
   */
  private static String determineFileFormat(BufferedInputStream bis)
          throws IOException {
    int maxLen = 1024 * 100;
    int len = 0;

    bis.mark(len);
    try {
      int[] magicNumber = new int[4];
      boolean onlyWhitespaceUntilNow = true;
      while (true) {
        int c = bis.read();
        ++len;
        if (c < 0 || len >= maxLen - 2) {
          return null;
        }

        if (len <= 4) {
          magicNumber[len - 1] = c;
        }

        if (!Character.isWhitespace(c) && onlyWhitespaceUntilNow) {
          onlyWhitespaceUntilNow = false;
          if (c == '[' || c == '{') {
            return JSON;
          } else if (c == '<') {
            return XML;
          }
        }
        if (len == 4 && magicNumber[0] == 'L' && magicNumber[1] == 'A'
                && magicNumber[2] == 'S' && magicNumber[3] == 'F') {
          return LAS;
        }
        if (!onlyWhitespaceUntilNow && len >= 4) {
          return null;
        }
      }
    } finally {
      bis.reset();
    }
  }
}
