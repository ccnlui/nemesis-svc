package nemesis.svc;

import com.aitusoftware.babl.user.Application;
import com.aitusoftware.babl.user.BroadcastSource;
import com.aitusoftware.babl.user.ContentType;
import com.aitusoftware.babl.websocket.DisconnectReason;
import com.aitusoftware.babl.websocket.SendResult;
import com.aitusoftware.babl.websocket.Session;
import com.aitusoftware.babl.websocket.broadcast.Broadcast;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.logbuffer.Header;

public class BablStreamServer implements Application, BroadcastSource
{
    private static Logger LOG = LoggerFactory.getLogger(BablStreamServer.class);
    private static int BROADCAST_TOPIC = 42;

    private final MutableDirectBuffer buffer = new ExpandableDirectByteBuffer(512);
    private Broadcast broadcastHandler;

    @Override
    public int onSessionConnected(Session session)
    {
        LOG.info("connected: {}", session.toString());
        broadcastHandler.addToTopic(BROADCAST_TOPIC, session.id());
        return SendResult.OK;
    }

    @Override
    public int onSessionDisconnected(Session session, DisconnectReason reason)
    {
        LOG.info("disconnected: {} reason: {}",
            session.toString(),
            reason.toString()
        );
        broadcastHandler.removeFromTopic(BROADCAST_TOPIC, session.id());
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
        this.broadcastHandler = broadcast;
    }

    public void createBroadcastTopic()
    {
        this.broadcastHandler.createTopic(BROADCAST_TOPIC);
    }

    public int broadcast(DirectBuffer buffer, int offset, int length, Header header)
    {
        return this.broadcastHandler.sendToTopic(BROADCAST_TOPIC, ContentType.TEXT, buffer, offset, length);
    }
}
