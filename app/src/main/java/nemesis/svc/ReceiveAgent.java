package nemesis.svc;

import java.nio.ByteBuffer;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import nemesis.svc.message.Message;
import nemesis.svc.message.Quote;
import nemesis.svc.message.Trade;

public class ReceiveAgent implements Agent
{
    private final Logger LOG = LoggerFactory.getLogger(ReceiveAgent.class);
    private final Subscription sub;
    private final UnsafeBuffer unsafeBuffer;
    private final Quote quote;
    private final Trade trade;
    private final FragmentHandler assembler;
    

    public ReceiveAgent(final Subscription sub)
    {
        this.sub = sub;
        this.unsafeBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(Message.MAX_SIZE));
        this.quote = new Quote();
        this.trade = new Trade();
        this.assembler = new FragmentAssembler(this::onMessage);
    }

    private void onMessage(DirectBuffer buffer, int offset, int length, Header header)
    {
        buffer.getBytes(offset, unsafeBuffer, 0, length);

        int msgType = unsafeBuffer.getByte(0);
        switch (msgType)
        {
        case Message.QUOTE:
            this.quote.fromByteBuffer(unsafeBuffer.byteBuffer());
            LOG.info("quote: {}", quote.receivedAt());
            break;

        case Message.TRADE:
            this.trade.fromByteBuffer(unsafeBuffer.byteBuffer());
            LOG.info("trade: {}", trade.receivedAt());
            break;

        default:
            LOG.error("unexpected message type: {}", msgType);
        }
    }

    @Override
    public int doWork() throws Exception
    {
        this.sub.poll(this.assembler, 10);
        return 0;
    }

    @Override
    public String roleName()
    {
        return "receiver";
    }
}
