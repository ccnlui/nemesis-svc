package nemesis.svc;

import nemesis.svc.command.Broadcaster;
import nemesis.svc.command.Connector;
import nemesis.svc.command.Counter;
import nemesis.svc.command.Listener;
import nemesis.svc.command.Marshaller;
import nemesis.svc.command.NyseServer;
import nemesis.svc.command.Publisher;
import nemesis.svc.command.Reader;
import nemesis.svc.command.StressClient;
import nemesis.svc.command.StressServer;
import nemesis.svc.command.Subscriber;
import nemesis.svc.command.ZeroGCListener;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(
    name = "nemesis",
    mixinStandardHelpOptions = true,
    usageHelpAutoWidth = true,
    description = "nemesis nanoservices")
public class Nemesis
{
    public static void main( String[] args )
    {
        CommandLine cmd = new CommandLine(new Nemesis());
        cmd.addSubcommand("listener", Listener.class);
        cmd.addSubcommand("reader", Reader.class);
        cmd.addSubcommand("publisher", Publisher.class);
        cmd.addSubcommand("subscriber", Subscriber.class);
        cmd.addSubcommand("stress-server", StressServer.class);
        cmd.addSubcommand("stress-client", StressClient.class);
        cmd.addSubcommand("counter", Counter.class);
        cmd.addSubcommand("zgc-listener", ZeroGCListener.class);
        cmd.addSubcommand("connector", Connector.class);
        cmd.addSubcommand("marshaller", Marshaller.class);
        cmd.addSubcommand("broadcaster", Broadcaster.class);
        cmd.addSubcommand("nyse-server", NyseServer.class);
        cmd.setCaseInsensitiveEnumValuesAllowed(true);

        int rc = cmd.execute(args);
        System.out.println("bye!");
        System.exit(rc);
    }
}
