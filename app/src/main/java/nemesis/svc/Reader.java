package nemesis.svc;

import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

import nemesis.svc.message.cqs.TransmissionBlock;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "reader",
    description = "read from ICE chronicle queue",
    usageHelpAutoWidth = true
)
public class Reader implements Callable<Void> {

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "help message")
    boolean help;

    @Override
    public Void call() throws Exception {

        final String queuePathIce = Config.queueBasePath + "/ice";

        try (
            SingleChronicleQueue inQueueIce = SingleChronicleQueueBuilder
                                                    .single(queuePathIce)
                                                    .rollCycle(Config.roleCycle)
                                                    .build();
        ) {
            final ExcerptTailer     tailer = inQueueIce.createTailer();
            final StringBuilder     sb     = new StringBuilder(32);
            final Bytes<ByteBuffer> bbb    = Bytes.elasticByteBuffer(TransmissionBlock.MAX_SIZE, TransmissionBlock.MAX_SIZE);

            System.out.println("Starting reader...");

            // Busy wait loop.
            while (true) {
                
                // read raw bytes into ByteBuffer
                // buf.clear();
                // tailer.readDocument(wire -> wire.readBytes(b -> b.read(buf)));
                
                // read event and bytes
                tailer.readDocument(wire -> {
                    sb.setLength(0);
                    wire.readEventName(sb).bytes(bbb, true);
                });
            }
        }
    }

    void printDebugString(Bytes<ByteBuffer> bbb) {
        System.out.println(bbb.toHexString());
        System.out.println(bbb.toDebugString());
        System.out.println("readLimit  " + bbb.readLimit());
        System.out.println("writeLimit " + bbb.writeLimit());
        System.out.println("readPosition " + bbb.readPosition());
        System.out.println("writePosition " + bbb.writePosition());
        System.out.println("capacity " + bbb.capacity());
        System.out.println("realCapacity " + bbb.realCapacity());
        System.out.println("start " + bbb.start());
    }
}
