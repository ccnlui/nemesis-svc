package nemesis.svc.command;

import java.util.concurrent.Callable;

import nemesis.svc.nanoservice.Config;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "stress-client",
    description = "subscribe to quote + trade messages",
    usageHelpAutoWidth = true)
public class StressClient implements Callable<Void>
{
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "help message")
    boolean help;

    @Option(names = "--embedded-media-driver", defaultValue = "false",
        description = "launch with embedded media driver (default ${DEFAULT-VALUE})")
    boolean embeddedMediaDriver;

    @Option(names = "--aeron-dir", description = "override directory name for embedded aeron media driver")
    String aeronDir;

    @Option(names = "--sub-endpoint", description = "aeron udp transport endpoint from which messages are subscribed <address:port>")
    String subEndpoint;

    @Option(names = "--duration", description = "duration of stress test in ns (default: 0)")
    long testDurationNs;

    @Option(names = "--metrics", description = "enable prometheus metrics server")
    boolean enableMetrics;

    @Override
    public Void call() throws Exception
    {
        mergeConfig();
        new nemesis.svc.nanoservice.StressClient().run();
        return null;
    }

    private void mergeConfig()
    {
        if (this.embeddedMediaDriver)
        {
            Config.embeddedMediaDriver = this.embeddedMediaDriver;
        }
        if (this.aeronDir != null && !this.aeronDir.isEmpty())
        {
            Config.aeronDir = this.aeronDir;
        }
        if (this.subEndpoint != null && !this.subEndpoint.isEmpty())
        {
            Config.subEndpoint = this.subEndpoint;
        }
        if (this.testDurationNs != 0)
        {
            Config.testDurationNs = this.testDurationNs;
        }
        if (this.enableMetrics)
        {
            Config.enableMetrics = this.enableMetrics;
        }
    }
}
