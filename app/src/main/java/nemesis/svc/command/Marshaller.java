package nemesis.svc.command;

import java.util.concurrent.Callable;
import nemesis.svc.message.Message;
import nemesis.svc.nanoservice.Config;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

@Command(
    name = "marshaller",
    usageHelpAutoWidth = true,
    description = "marshall nemesis trades and quotes messages")
public class Marshaller implements Callable<Void>
{
    @Spec CommandSpec spec;

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "help message")
    boolean help;

    @Option(names = "--embedded-media-driver", defaultValue = "false",
        description = "launch with embedded media driver (default ${DEFAULT-VALUE})")
    boolean embeddedMediaDriver;

    @Option(names = "--aeron-dir", description = "override directory name for embedded aeron media driver")
    String aeronDir;

    @Option(names = "--pub-endpoint", description = "aeron udp transport endpoint to which messages are published <address:port>")
    String pubEndpoint;

    @Option(names = "--sub-endpoint", description = "aeron udp transport endpoint from which messages are subscribed <address:port>")
    String subEndpoint;

    @Option(names = "--format", description = "message format: json or msgpack")
    Message.Format format;

    @Override
    public Void call() throws Exception
    {
        mergeConfig();
        new nemesis.svc.nanoservice.Marshaller().run();
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
        if (this.subEndpoint != null && !this.subEndpoint.isEmpty())
        {
            Config.subEndpoint = this.subEndpoint;
        }
        if (this.format != null)
        {
            Config.messageFormat = this.format;
        }
    }
}
