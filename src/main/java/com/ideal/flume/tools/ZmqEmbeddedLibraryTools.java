package com.ideal.flume.tools;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

import org.zeromq.ZMQ;

public class ZmqEmbeddedLibraryTools {
  public static final boolean LOADED_EMBEDDED_LIBRARY;

  static {
    LOADED_EMBEDDED_LIBRARY = loadEmbeddedLibrary();

    if (!LOADED_EMBEDDED_LIBRARY) {
      throw new RuntimeException(
          "there is no zmq lib in " + ZmqEmbeddedLibraryTools.getCurrentPlatformIdentifier());
    }
  }

  public static String getCurrentPlatformIdentifier() {
    String osName = System.getProperty("os.name");
    if (osName.toLowerCase().indexOf("windows") > -1) {
      osName = "Windows";
    }

    return System.getProperty("os.arch") + "/" + osName;
  }

  private static boolean loadEmbeddedLibrary() {
    boolean usingEmbedded = false;

    // attempt to locate embedded native library within JAR at following
    // location:
    // /NATIVE/${os.arch}/${os.name}/libjzmq.[so|dylib|dll]
    String[] allowedExtensions = new String[] {"so", "dylib", "dll"};
    StringBuilder url = new StringBuilder();
    url.append("/NATIVE/");
    url.append(getCurrentPlatformIdentifier());
    url.append("/libzmq.");
    URL nativeLibraryUrl = null;
    // loop through extensions, stopping after finding first one
    for (String ext : allowedExtensions) {
      nativeLibraryUrl = ZMQ.class.getResource(url.toString() + ext);
      if (nativeLibraryUrl != null)
        break;
    }

    if (nativeLibraryUrl != null) {

      // native library found within JAR, extract and load

      try {

        final File libfile = File.createTempFile("libzmq-", ".lib");
        libfile.deleteOnExit(); // just in case

        final InputStream in = nativeLibraryUrl.openStream();
        final OutputStream out = new BufferedOutputStream(new FileOutputStream(libfile));

        int len = 0;
        byte[] buffer = new byte[8192];
        while ((len = in.read(buffer)) > -1)
          out.write(buffer, 0, len);
        out.close();
        in.close();

        System.load(libfile.getAbsolutePath());

        libfile.delete();

        usingEmbedded = true;

      } catch (IOException x) {
        // mission failed, do nothing
      }

    } // nativeLibraryUrl exists

    return usingEmbedded;
  }
}
