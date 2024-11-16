package command;

import config.ObjectFactory;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Base64;
import java.util.List;

public class Psync extends AbstractHandler {
    private static final String EMPTY_RDB_FILE =
            "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

    public Psync(ObjectFactory objectFactory) {
        super(objectFactory);
    }

    @Override
    public byte[] handle(String[] args) {
        String replicationId = ObjectFactory.getInstance().getProperties().getReplicationId();
        long replicationOffset = ObjectFactory.getInstance().getProperties().getReplicationOffset();
        String response = String.format("FULLRESYNC %s %s", replicationId, replicationOffset);
        byte[] fullRsyncResponseBytes = ObjectFactory.getInstance().getProtocolSerializer().simpleString(response);
        byte[] rdbFile = Base64.getDecoder().decode(EMPTY_RDB_FILE);
        byte[] sizePrefix = ("$" + rdbFile.length + "\r\n").getBytes();
        byte[] responseBytes = ArrayUtils.addAll(fullRsyncResponseBytes, sizePrefix);
        responseBytes = ArrayUtils.addAll(responseBytes, rdbFile);
        return responseBytes;
    }
}
