package nemesis.svc;

import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import nemesis.svc.message.Message;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;

final class Config
{
    private Config()
    {
    }

    // Chronicle
    static final String queueBasePath = "/queue";
    static final RollCycle roleCycle = RollCycles.MINUTELY;

    // Media driver
    static boolean embeddedMediaDriver = false;
    static String aeronDir = "";

    // Aeron
    static String pubEndpoint = "";
    static String subEndpoint = "";
    static IdleStrategy idleStrategy = new BusySpinIdleStrategy();
    
    // Streams
    static int exchangeDataStream = 9000;
    static int pipedDataStream = 9001;
    static int websocketDataStream = 9002;

    // Marshaller
    static Message.Format messageFormat = Message.Format.MSGPACK;

    // Broadcaster
    static int websocketPort = 8080;
    static String websocketLib = "babl";
    static String bablConfigPath = System.getProperty("user.dir") + "/build/resources/main/babl-default.properties";

    // Stress server
    static long quoteIntervalUs = 100;
    static long tradeIntervalUs = 200;
}
