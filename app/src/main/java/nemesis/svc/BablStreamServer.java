package nemesis.svc;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

import com.aitusoftware.babl.config.BablConfig;
import com.aitusoftware.babl.config.PropertiesLoader;
import com.aitusoftware.babl.user.Application;
import com.aitusoftware.babl.user.BroadcastSource;
import com.aitusoftware.babl.user.ContentType;
import com.aitusoftware.babl.websocket.BablServer;
import com.aitusoftware.babl.websocket.DisconnectReason;
import com.aitusoftware.babl.websocket.SendResult;
import com.aitusoftware.babl.websocket.Session;
import com.aitusoftware.babl.websocket.SessionContainers;
import com.aitusoftware.babl.websocket.broadcast.Broadcast;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BablStreamServer implements Application, BroadcastSource
{
    private static Logger LOG = LoggerFactory.getLogger(BablStreamServer.class);

    private final MutableDirectBuffer buffer = new ExpandableDirectByteBuffer(512);
    private Broadcast broadcast;

    public static void main(String[] args) throws InterruptedException
    {
        String configPath = "/Users/calvin/source/java/nemesis-svc/babl-performance.properties";
        final BablConfig config = PropertiesLoader
            .configure(Paths.get(configPath));
        BablStreamServer streamServer = new BablStreamServer();
        config.applicationConfig().application(streamServer);
        try (SessionContainers containers = BablServer.launch(config))
        {
            containers.start();

            streamServer.broadcast.createTopic(42);
            final String TOPIC_ONE_MSG = "TOPIC_ONE";
            final UnsafeBuffer topicOneMsg = new UnsafeBuffer(TOPIC_ONE_MSG.getBytes(StandardCharsets.UTF_8));

            while (true)
            {
                streamServer.broadcast.sendToTopic(42, ContentType.TEXT, topicOneMsg, 0, topicOneMsg.capacity());
                Thread.sleep(1);
            }

            // new ShutdownSignalBarrier().await();
        }
    }

    @Override
    public int onSessionConnected(Session session)
    {
        LOG.info("connected: {}", session.toString());
        broadcast.addToTopic(42, session.id());
        return SendResult.OK;
    }

    @Override
    public int onSessionDisconnected(Session session, DisconnectReason reason)
    {
        LOG.info("disconnected: {} reason: {}",
            session.toString(),
            reason.toString()
        );
        broadcast.removeFromTopic(42, session.id());
        return SendResult.OK;
    }

    @Override
    public int onSessionMessage(Session session, ContentType contentType, DirectBuffer msg, int offset, int length)
    {
        LOG.info("onSessionMessage: {}: {}",
            session.toString(),
            contentType.toString()
        );
        buffer.putBytes(0, msg, offset, length);
        int sendResult;
        do
        {
            sendResult = session.send(contentType, buffer, 0, length);
        }
        while (sendResult != SendResult.OK);

        return sendResult;
    }

    @Override
    public void setBroadcast(Broadcast broadcast)
    {
        this.broadcast = broadcast;
    }
}
