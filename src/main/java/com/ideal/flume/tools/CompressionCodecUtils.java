package com.ideal.flume.tools;

import com.google.common.collect.Maps;
import com.ideal.flume.clients.CollectClient;
import com.ideal.flume.clients.HdfsCollectClient;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;

public class CompressionCodecUtils {
  private static final String[] codecs = new String[] {"org.apache.hadoop.io.compress.DefaultCodec",
      "org.apache.hadoop.io.compress.GzipCodec", "org.apache.hadoop.io.compress.BZip2Codec",
      "org.apache.hadoop.io.compress.DeflateCodec", "com.hadoop.compression.lzo.LzoCodec",
      "com.hadoop.compression.lzo.LzopCodec", "org.apache.hadoop.io.compress.SnappyCodec",
      "org.apache.hadoop.io.compress.Lz4Codec"};

  private final SortedMap<String, CompressionCodec> codecsMap = Maps.newTreeMap();

  private final Map<String, CompressionCodec> codecsByName = Maps.newHashMap();

  private final Map<String, CompressionCodec> codecsByClassName = Maps.newHashMap();

  /**
   * 构造器.
   * 
   * @param fileSystem RedcamFileSystem
   */
  public CompressionCodecUtils(CollectClient fileClient) {
    Configuration conf;
    if (fileClient instanceof HdfsCollectClient) {
      conf = ((HdfsCollectClient) fileClient).getConfHdfs();
    } else {
      conf = new Configuration();
      conf.set("io.compression.codecs", StringUtils.join(codecs, ','));
    }

    initCodecs(conf);
  }

  private void initCodecs(Configuration conf) {
    List<Class<? extends CompressionCodec>> codecClasses =
        CompressionCodecFactory.getCodecClasses(conf);
    if (null == codecClasses || codecClasses.isEmpty()) {
      addCodec(new GzipCodec());
      addCodec(new DefaultCodec());
    } else {
      for (Class<? extends CompressionCodec> codecClass : codecClasses) {
        addCodec(ReflectionUtils.newInstance(codecClass, conf));
      }
    }
  }

  private void addCodec(CompressionCodec codec) {
    String suffix = codec.getDefaultExtension();
    codecsMap.put(new StringBuilder(suffix).reverse().toString(), codec);
    codecsByClassName.put(codec.getClass().getCanonicalName(), codec);

    String codecName = codec.getClass().getSimpleName();
    codecsByName.put(codecName.toLowerCase(), codec);
    if (codecName.endsWith("Codec")) {
      codecName = codecName.substring(0, codecName.length() - "Codec".length());
      codecsByName.put(codecName.toLowerCase(), codec);
    }
  }

  /**
   * Find the relevant compression codec for the given file based on its filename suffix.
   * 
   * @param file the filename to check
   * @return the codec object
   */
  public CompressionCodec getCodec(Path file) {
    CompressionCodec result = null;
    if (!codecsMap.isEmpty()) {
      String filename = file.getName();
      String reversedFilename = new StringBuilder(filename).reverse().toString();
      SortedMap<String, CompressionCodec> subMap = codecsMap.headMap(reversedFilename);
      if (!subMap.isEmpty()) {
        String potentialSuffix = subMap.lastKey();
        if (reversedFilename.startsWith(potentialSuffix)) {
          result = codecsMap.get(potentialSuffix);
        }
      }
    }

    return result;
  }

  /**
   * Find the relevant compression codec for the codec's canonical class name or by codec alias.
   * <p/>
   * Codec aliases are case insensitive.
   * <p/>
   * The code alias is the short class name (without the package name). If the short class name ends
   * with 'Codec', then there are two aliases for the codec, the complete short class name and the
   * short class name without the 'Codec' ending. For example for the 'GzipCodec' codec class name
   * the alias are 'gzip' and 'gzipcodec'.
   *
   * @param codecName the canonical class name of the codec
   * @return the codec object
   */
  public CompressionCodec getCodec(String codecName) {
    if (codecsByClassName.isEmpty() || StringUtils.isBlank(codecName)) {
      return null;
    }
    CompressionCodec codec = getCodecByClassName(codecName);
    if (null == codec) {
      codec = codecsByName.get(codecName.toLowerCase());
    }
    return codec;
  }

  /**
   * Find the relevant compression codec for the codec's canonical class name.
   * 
   * @param classname the canonical class name of the codec
   * @return the codec object
   */
  public CompressionCodec getCodecByClassName(String classname) {
    if (codecsByClassName.isEmpty()) {
      return null;
    }
    return codecsByClassName.get(classname);
  }

}
