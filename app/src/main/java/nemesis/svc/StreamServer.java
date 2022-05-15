package nemesis.svc;

import java.net.InetSocketAddress;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamServer extends WebSocketServer
{
    private static Logger LOG = LoggerFactory.getLogger(StreamServer.class);

    StreamServer(int port)
    {
        super(new InetSocketAddress(port));
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake)
    {
        LOG.info("connected: {}", conn.getRemoteSocketAddress().toString());
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote)
    {
        LOG.info("disconnected: {}: code: {} reason: {}",
            conn.getRemoteSocketAddress().toString(),
            reason
        );
    }

    @Override
    public void onMessage(WebSocket conn, String message)
    {
        LOG.info("onMessage: {}: {}",
            conn.getRemoteSocketAddress().toString(),
            message
        );
    }

    @Override
    public void onError(WebSocket conn, Exception ex)
    {
        LOG.error("onError: {}: {}", conn.getRemoteSocketAddress().toString(), ex.toString());
    }

    @Override
    public void onStart()
    {
        LOG.info("websocket server running on {}", getAddress());
    }
}
