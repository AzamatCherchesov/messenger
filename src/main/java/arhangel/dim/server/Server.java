package arhangel.dim.server;

import arhangel.dim.container.Context;
import arhangel.dim.container.InvalidConfigurationException;
import arhangel.dim.core.command.CreateChatCommand;
import arhangel.dim.core.command.HistChatCommand;
import arhangel.dim.core.command.InfoCommand;
import arhangel.dim.core.command.ListChatCommand;
import arhangel.dim.core.command.LoginCommand;
import arhangel.dim.core.command.RegisterCommand;
import arhangel.dim.core.command.TextCommand;
import arhangel.dim.core.dbservice.dao.UsersDao;
import arhangel.dim.core.messages.CommandExecutor;
import arhangel.dim.core.messages.Type;
import arhangel.dim.core.net.Protocol;
import arhangel.dim.core.net.Session;
import arhangel.dim.core.store.MessageStore;
import arhangel.dim.core.store.MessageStoreImpl;
import arhangel.dim.core.store.UserStore;
import arhangel.dim.core.store.UserStoreImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.sql.SQLException;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Основной класс для сервера сообщений
 */
public class Server {

    private static Logger log = LoggerFactory.getLogger(Server.class);
    private int bufferSize = 256 * 32;
    private int port;
    private Protocol protocol;
    private final ExecutorService threadPool = Executors.newCachedThreadPool();
    private Set<Session> sessions = new HashSet<>();
    private CommandExecutor commandExecutor = new CommandExecutor();

    private UsersDao usersDao = new UsersDao();
    private MessageStore messageStore = new MessageStoreImpl(usersDao);
    private UserStore userStore = new UserStoreImpl(usersDao);
    private AsynchronousChannelGroup channelGroup;
    private AsynchronousServerSocketChannel serverSocketChannel;


    public Server(){}

    public void init( ) throws IOException {
        try {
            usersDao.init();
            this.commandExecutor.addCommand(Type.MSG_REGISTER, new RegisterCommand());
            this.commandExecutor.addCommand(Type.MSG_LOGIN, new LoginCommand(this));
            this.commandExecutor.addCommand(Type.MSG_INFO, new InfoCommand(this));
            this.commandExecutor.addCommand(Type.MSG_CHAT_CREATE, new CreateChatCommand(this));
            this.commandExecutor.addCommand(Type.MSG_CHAT_HIST, new HistChatCommand(this));
            this.commandExecutor.addCommand(Type.MSG_TEXT, new TextCommand(this));
            this.commandExecutor.addCommand(Type.MSG_CHAT_LIST, new ListChatCommand(this));

            channelGroup = AsynchronousChannelGroup.withThreadPool(threadPool);
            serverSocketChannel = AsynchronousServerSocketChannel.open(channelGroup);
            serverSocketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            serverSocketChannel.bind(new InetSocketAddress(port));

        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    public void start() {
        AcceptCompletionHandler acceptCompletionHandler = new AcceptCompletionHandler(this, serverSocketChannel);
        serverSocketChannel.accept(null, acceptCompletionHandler);
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            log.info("[run] Main thread interrupted");
        }
    }

    public void stop() {

    }

    public int getBufferSize() {
        return bufferSize;
    }

    public AsynchronousServerSocketChannel getServerSocketChannel() {
        return serverSocketChannel;
    }

    public AsynchronousChannelGroup getChannelGroup() {
        return channelGroup;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public Protocol getProtocol() {
        return protocol;
    }

    public void setIProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    public void  setExecutor(CommandExecutor executor) {
        this.commandExecutor = executor;
    }

    public CommandExecutor getExecutor() {
        return commandExecutor;
    }

    public void setUsersDao(UsersDao usersDao) {
        this.usersDao = usersDao;
    }

    public UsersDao getUsersDao() {
        return usersDao;
    }

    public void setMessageStore(MessageStore messageStore) {
        this.messageStore = messageStore;
    }

    public MessageStore getMessageStore() {
        return messageStore;
    }

    public void setUserStore(UserStore userStore) {
        this.userStore = userStore;
    }

    public UserStore getUserStore() {
        return userStore;
    }

    public void setSessions(Set<Session> sessions) {
        this.sessions = sessions;
    }

    public Set<Session> getSessions() {
        return sessions;
    }



    public static void main(String[] args) throws Exception {
        Server server = null;
        // Пользуемся механизмом контейнера
        try {
            Context context = new Context("server.xml");
            server = (Server) context.getBeanByName("server");
        } catch (InvalidConfigurationException e) {
            log.error("Failed to create server: configuration error", e);
            return;
        }
        server.init();

        //Protocol stringProtocol = new StringProtocol();
        //Server server = new Server(19000);
        //server.setIProtocol(stringProtocol);
        server.start();
    }


}