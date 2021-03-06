package arhangel.dim.core.dbservice.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import arhangel.dim.core.User;
import arhangel.dim.core.dbservice.executor.ResultHandler;
import arhangel.dim.core.messages.Message;
import arhangel.dim.core.Chat;
import arhangel.dim.core.messages.TextMessage;
import arhangel.dim.core.messages.Type;
import org.postgresql.ds.PGPoolingDataSource;
import arhangel.dim.core.dbservice.executor.QueryExecutor;

public class UsersDao {
    QueryExecutor queryExecutor;

    public UsersDao() {}

    // database access test
    public static void main(String[] args) throws Exception {
        UsersDao usersDao = new UsersDao();
        usersDao.init();

        //Long num = new Long(0);
        usersDao.createUserchats();
        usersDao.addUserToChat(new Long(0),new Long(1));

    }

    public void init() throws SQLException, ClassNotFoundException {

        Class.forName("org.postgresql.Driver");

        PGPoolingDataSource source = new PGPoolingDataSource();
        source.setDataSourceName("jdbc:postgresql");
        source.setServerName("178.62.140.149");
        source.setDatabaseName("AzamatCherchesov");
        source.setUser("trackuser");
        source.setPassword("trackuser");
        source.setMaxConnections(10);

        Connection connection = source.getConnection();

        queryExecutor = new QueryExecutor();
        queryExecutor.setConnection(connection);
        String sql1 = "CREATE TABLE IF NOT EXISTS users (" +
                "id BIGSERIAL PRIMARY KEY," +
                "login VARCHAR(255) UNIQUE," +
                "password VARCHAR(255)" +
                ");";
        queryExecutor.updateQuery(sql1);
        String sq2 = "CREATE TABLE IF NOT EXISTS userchats(user_id BIGINT, chat_id BIGINT);";
        queryExecutor.updateQuery(sq2);
        String sq3 = "CREATE TABLE IF NOT EXISTS chats(chat_id BIGSERIAL PRIMARY KEY, owner_id BIGINT);";
        queryExecutor.updateQuery(sq3);
        String sq4 = "CREATE TABLE IF NOT EXISTS messages(msg_id BIGSERIAL PRIMARY KEY, chat_id BIGINT," +
                " msg_text TEXT, sender_id BIGINT);";
        queryExecutor.updateQuery(sq4);
    }

    public void createUserchats() {
        try {


            String sql = "DROP TABLE userchats;";
            queryExecutor.updateQuery(sql);
         //   sql = "" +
          //          "CREATE TABLE userchats(user_id BIGINT, chat_id BIGINT);";
           // queryExecutor.updateQuery(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public User getUser(String userName) throws Exception {

        String sql = "" +
                "SELECT id, login, password " +
                "FROM users " +
                "WHERE login = ?";

        Map<Integer, Object> prepared = new HashMap<>();
        prepared.put(1, userName);

        User user = queryExecutor.execQuery(sql, prepared, new ResultHandler<User>() {
            @Override
            public User handle(ResultSet resultSet) throws SQLException {
                while (resultSet.next()) {
                    String login = resultSet.getString("login");
                    String hash = resultSet.getString("password");
                    Long id = resultSet.getLong("id");
                    User user = new User(login);
                    user.setPass(hash);
                    user.setId(id);
                    return user;
                }
                return null;
            }
        });

        return user;
    }

    public User getUserById(Long id) throws Exception {
        String sql = "" +
                "SELECT login, password " +
                "FROM users " +
                "WHERE id = ? ";

        Map<Integer, Object> prepared = new HashMap<>();
        prepared.put(1, id);

        User user = queryExecutor.execQuery(sql, prepared, new ResultHandler<User>() {

            public User handle(ResultSet resultSet) throws SQLException {
                while (resultSet.next()) {
                    String login = resultSet.getString("login");
                    String hash = resultSet.getString("password");
                    User user = new User(login);
                    user.setHash(hash);
                    return user;
                }
                return null;
            }
        });
        if (user != null) {
            user.setId(id);
        }
        return user;
    }

    public void setNewPass(String login, String password) throws Exception {

        String sqlUpdate = "" +
                "UPDATE users " +
                "SET password = ? " +
                "WHERE login = ? ;";

        Map<Integer, Object> prepared = new HashMap<>();
        prepared.put(1, login);
        prepared.put(2, password);

        queryExecutor.updateQuery(sqlUpdate, prepared);
    }

    public void addChat(Chat chat) {

        String sqlInsert = "INSERT INTO chats (owner_id) VALUES " +
                "(?) ;";

        Map<Integer, Object> prepared = new HashMap<>();
        prepared.put(1, chat.getCreatorId());

        Long key = null;
        try {
            key = queryExecutor.updateQueryWithGeneratedKey(sqlInsert, prepared, "chat_id");
            System.out.println("key = " + key);
        } catch (SQLException sqlExc) {
            System.err.println("troubles with sql query=" + sqlInsert + ", where ?=" + chat.getCreatorId());
            sqlExc.printStackTrace();
        }

        chat.setId(key);

    }

    public void addUserToChat(Long userId, Long chatId) {

        String sqlInsert = "INSERT INTO userchats (\"chat_id\", \"user_id\") " +
                "VALUES " +
                "(?, ?); ";

        Map<Integer, Object> prepared = new HashMap<>();
        prepared.put(1, chatId);
        prepared.put(2, userId);

        try {
            queryExecutor.updateQuery(sqlInsert, prepared);
        } catch (SQLException sqlExc) {
            System.err.println("some troubles occured while parsing this:");
            System.err.println(sqlInsert);
            System.err.println("where ?=" + chatId + ", ?=" + userId);
            sqlExc.printStackTrace();
        }
    }

    public List<Long> getChatsByUserId(Long userId) {

        String sql = "SELECT chat_id " +
                "FROM userchats " +
                "WHERE user_id = ?";

        Map<Integer, Object> prepared = new HashMap<>();
        prepared.put(1, userId);

        List<Long> chatList = null;
        try {
            chatList = queryExecutor.execQuery(sql, prepared, new ResultHandler<List<Long>>() {

                public List<Long> handle(ResultSet resultSet) throws SQLException {
                    List<Long> chatList = new ArrayList<Long>();
                    while (resultSet.next()) {
                        chatList.add(resultSet.getLong("chat_id"));
                    }
                    return chatList;
                }
            });
        } catch (SQLException sqlExc) {
            System.err.println("some troubles with sql=\n" + sql);
            System.err.println("?=" + userId);
            sqlExc.printStackTrace();
        }
        return chatList;
    }

    public List<Long> getUsersByChatId(Long chatId) {
        String sql = "SELECT user_id " +
                "FROM userchats " +
                "WHERE chat_id = ?";

        Map<Integer, Object> prepared = new HashMap<>();
        prepared.put(1, chatId);

        List<Long> userList = null;
        try {
            userList = queryExecutor.execQuery(sql, prepared, new ResultHandler<List<Long>>() {

                public List<Long> handle(ResultSet resultSet) throws SQLException {
                    List<Long> userList = new ArrayList<Long>();
                    while (resultSet.next()) {
                        userList.add(resultSet.getLong("user_id"));
                    }
                    return userList;
                }
            });
        } catch (SQLException sqlExc) {
            System.err.println("some troubles with sql=\n" + sql);
            System.err.println("?=" + chatId);
            sqlExc.printStackTrace();
        }
        return userList;
    }

    public void addMessage(Long chatId, Message msg) {

        String sqlInsert = "INSERT INTO messages " +
                "(msg_text, chat_id, sender_id) VALUES " +
                "(?, ?, ?) ";

        Map<Integer, Object> prepared = new HashMap<>();
        prepared.put(1, ((TextMessage)msg).getText());
        prepared.put(2, chatId);
        prepared.put(3, msg.getSenderId());

        try {
            Long id = queryExecutor.updateQueryWithGeneratedKey(sqlInsert, prepared, "msg_id");
            msg.setId(id);
        } catch (SQLException sqlExc) {
            System.err.println("troubles occured with parsign sql=\n");
            System.err.println(sqlInsert);
            System.err.println("?=" + prepared.get(1));
            System.err.println("?=" + prepared.get(2));
            System.err.println("?=" + prepared.get(3));
            System.err.println("?=" + prepared.get(4));
            sqlExc.printStackTrace();
        }
    }

    public List<Long> getMessagesByChatId(Long chatId) {

        String sql = "SELECT msg_id " +
                "FROM messages " +
                "WHERE chat_id = ?";

        Map<Integer, Object> prepared = new HashMap<>();
        prepared.put(1, chatId);

        List<Long> msgList = null;
        try {
            msgList = queryExecutor.execQuery(sql, prepared, new ResultHandler<List<Long>>() {
                @Override
                public List<Long> handle(ResultSet resultSet) throws SQLException {
                    List<Long> msgList = new ArrayList<Long>();
                    while (resultSet.next()) {
                        msgList.add(resultSet.getLong("msg_id"));
                    }
                    return msgList;
                }
            });
        } catch (SQLException sqlExc) {
            System.err.println("some troubles with sql=\n" + sql);
            System.err.println("?=" + chatId);
            sqlExc.printStackTrace();
        }
        return msgList;

    }

    public Message getMessageById(Long messageId) {

        String sql = "" +
                "SELECT * " +
                "FROM messages " +
                "WHERE msg_id = ?";

        Map<Integer, Object> prepared = new HashMap<>();
        prepared.put(1, messageId);
        Message msg = null;
        try {
            msg = queryExecutor.execQuery(sql, prepared, new ResultHandler<Message>() {
                @Override
                public TextMessage handle(ResultSet resultSet) throws SQLException {
                    TextMessage msg = new TextMessage();
                    while (resultSet.next()) {
                        msg.setId(resultSet.getLong("msg_id"));
                        msg.setText(resultSet.getString("msg_text"));
                        msg.setType(Type.MSG_TEXT);
                        msg.setSenderId(resultSet.getLong("sender_id"));

                        return msg;
                    }
                    return null;
                }
            });
        } catch (SQLException sqlExc) {
            System.err.println("some troubles occured while parsing sql=\n" + sql);
            System.err.println("where ?=" + messageId);
            sqlExc.printStackTrace();
        }
        return msg;
    }

    public User addUser(String userName, String password) throws Exception {

        User user;

        String sqlInsert = "INSERT INTO users (\"login\", \"password\") VAlUES " +
                "(?, ?);";

        Map<Integer, Object> prepared = new HashMap<>();
        prepared.put(1, userName);
        prepared.put(2, new String(password));

        Long id = queryExecutor.updateQueryWithGeneratedKey(sqlInsert, prepared, "id");

        user = new User(userName);
        user.setHash(password);
        user.setId(id);
        return user;

    }

    public void close() {
        queryExecutor.close();
    }
}