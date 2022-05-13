package nemesis.svc;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(
    name = "nemesis",
    mixinStandardHelpOptions = true,
    description = "nemesis nanoservices",
    usageHelpAutoWidth = true
)
public class Nemesis {
    
    public static void main( String[] args ) {

        CommandLine cmd = new CommandLine(new Nemesis());
        cmd.addSubcommand("listener", Listener.class);
        cmd.addSubcommand("reader", Reader.class);
        cmd.addSubcommand("publisher", Publisher.class);
        cmd.addSubcommand("subscriber", Subscriber.class);
        cmd.addSubcommand("stress-server", StressServer.class);
        cmd.addSubcommand("stress-client", StressClient.class);
        cmd.addSubcommand("counter", Counter.class);
        cmd.addSubcommand("zgc-listener", ZeroGCListener.class);
        System.exit(cmd.execute(args));

    }

}
