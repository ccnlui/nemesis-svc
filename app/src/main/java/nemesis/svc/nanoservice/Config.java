package nemesis.svc.nanoservice;

import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import nemesis.svc.message.Message;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;

public final class Config
{
    private Config()
    {
    }

    // Chronicle
    public static final String queueBasePath = "/queue";
    public static final RollCycle roleCycle = RollCycles.MINUTELY;

    // Media driver
    public static boolean embeddedMediaDriver = false;
    public static String aeronDir = "";

    // Aeron
    public static String pubEndpoint = "";
    public static String subEndpoint = "";
    public static IdleStrategy idleStrategy = new BusySpinIdleStrategy();
    
    // Streams
    public static int exchangeDataStream = 9000;
    public static int pipedDataStream = 9001;
    public static int websocketDataStream = 9002;

    // Marshaller
    public static Message.Format messageFormat = Message.Format.MSGPACK;

    // Broadcaster
    public static int websocketPort = 8080;
    public static String websocketLib = "babl";
    public static String bablConfigPath = System.getProperty("user.dir") + "/build/resources/main/babl-default.properties";

    // Stress server
    public static long quoteIntervalUs = 100;
    public static long tradeIntervalUs = 200;
}