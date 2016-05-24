package arhangel.dim.core.net;

import java.io.IOException;
import arhangel.dim.core.User;
import arhangel.dim.core.messages.CommandException;
import arhangel.dim.core.messages.Message;
import arhangel.dim.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import arhangel.dim.server.WriteCompletionHandler;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;

/**
 * Здесь храним всю информацию, связанную с отдельным клиентом.
 * - объект User - описание пользователя
 * - сокеты на чтение/запись данных в канал пользователя
 */
public class Session implements ConnectionHandler {
    static Logger log = LoggerFactory.getLogger(Session.class);
    private Server server;
    private AsynchronousSocketChannel asynchronousSocketChannel;
    private User user;

    public AsynchronousSocketChannel getAsynchronousSocketChannel() {
        return asynchronousSocketChannel;
    }

    public void setAsynchronousSocketChannel(AsynchronousSocketChannel asynchronousSocketChannel) {
        this.asynchronousSocketChannel = asynchronousSocketChannel;
    }

    public Server getServer() {
        return server;
    }

    public void setServer(Server server) {
        this.server = server;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    public Session(Server server) {
        this.server = server;
    }

    @Override
    public void send(Message msg) throws ProtocolException, IOException {
        asynchronousSocketChannel.write(ByteBuffer.wrap(server.getProtocol().encode(msg)),
                this,
                new WriteCompletionHandler(server));
    }

    @Override
    public void onMessage(Message msg) {
        log.info("Handling message: {}", msg);
        try {
            server.getExecutor().handleMessage(msg, this);
        } catch (CommandException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        log.info("[close] Closing session with {}", user);
        try {
            asynchronousSocketChannel.close();
        } catch (IOException e) {
            log.error("[close] Couldn't close socket channel, fuck it", e);
        }
    }

    public boolean userAuthenticated() {
        return (user != null);
    }
}