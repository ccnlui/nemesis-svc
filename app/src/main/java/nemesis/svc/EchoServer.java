package nemesis.svc;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "echoserver",
    description = "udp multicast echo server",
    usageHelpAutoWidth = true
)
class EchoServer implements Callable<Void> {

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "help message")
    boolean help;

    private DatagramSocket socket;
    private InetAddress group;
    private byte[] buf;

    @Override
    public Void call() throws Exception {
        
        System.out.println("echo server...");
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        ScheduledFuture<?> res = executor.scheduleAtFixedRate(() -> {
            try {
                String msg = Instant.now().toString();
                // System.out.println(msg);
                multicast(msg);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 0, 1, TimeUnit.NANOSECONDS);
        res.get();

        return null;
    }

    void multicast(String msg) throws IOException {
        socket = new DatagramSocket();
        group = InetAddress.getByName("230.0.0.0");
        buf = msg.getBytes();

        DatagramPacket p = new DatagramPacket(buf, buf.length, group, 4446);
        socket.send(p);
        socket.close();
    }
}
