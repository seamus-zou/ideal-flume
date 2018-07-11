package com.ideal.flume.clients;

import com.ideal.flume.enums.ClientType;

import org.apache.commons.lang3.StringUtils;

public class CollectClientFactory {
  public static CollectClient createClient(ClientProps props) throws Exception {
    if (props.getType() == ClientType.LOCAL) {
      return new LocalCollectClient();
    }

    if (props.getType() == ClientType.FTP) {
      return new FtpCollectClient(props);
    }

    if (props.getType() == ClientType.SFTP) {
      return new SftpCollectClient(props);
    }

    if (props.getType() == ClientType.HDFS) {
      if (null != props.getHdfsConfig()) {
        return new HdfsCollectClient(props.getHdfsConfig());
      } else if (StringUtils.isNotBlank(props.getPrincipal())
          && (StringUtils.isNotBlank(props.getTicketCache())
              || StringUtils.isNotBlank(props.getKeytab()))) {
        return new HdfsKbsCollectClient(props.getTicketCache(), props.getKeytab(),
            props.getPrincipal());
      }
    }

    throw new IllegalArgumentException("unsupported client type.");
  }
}
