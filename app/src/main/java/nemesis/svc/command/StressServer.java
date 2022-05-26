package nemesis.svc.command;

import java.util.concurrent.Callable;

import nemesis.svc.nanoservice.Config;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "stress-server",
    description = "produce and publish quote + trade messages",
    usageHelpAutoWidth = true)
public class StressServer implements Callable<Void>
{
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "help message")
    boolean help;

    @Option(names = "--embedded-media-driver", defaultValue = "false",
        description = "launch with embedded media driver (default ${DEFAULT-VALUE})")
    boolean embeddedMediaDriver;

    @Option(names = "--aeron-dir", description = "directory name for aeron media driver")
    String aeronDir;

    @Option(names = "--pub-endpoint", description = "aeron udp transport endpoint to which messages are published <address:port>")
    String pubEndpoint;

    @Option(names = "--quote-interval", description = "set the interval for producing quotes")
    long quoteIntervalUs;

    @Option(names = "--trade-interval",description = "set the interval for producing trades")
    long tradeIntervalUs;

    @Override
    public Void call() throws Exception
    {
        mergeConfig();
        new nemesis.svc.nanoservice.StressServer().run();
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
        if (this.pubEndpoint != null && !this.pubEndpoint.isEmpty())
        {
            Config.pubEndpoint = this.pubEndpoint;
        }
        if (this.quoteIntervalUs != 0)
        {
            Config.quoteIntervalUs = this.quoteIntervalUs;
        }
        if (this.tradeIntervalUs != 0)
        {
            Config.tradeIntervalUs = this.tradeIntervalUs;
        }
    }
}
