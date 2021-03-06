package arhangel.dim.core.net;

import arhangel.dim.core.messages.CreateChatMessage;
import arhangel.dim.core.messages.HistChatMessage;
import arhangel.dim.core.messages.HistChatResultMessage;
import arhangel.dim.core.messages.InfoMessage;
import arhangel.dim.core.messages.ListChatMessage;
import arhangel.dim.core.messages.ListChatResultMessage;
import arhangel.dim.core.messages.LoginMessage;
import arhangel.dim.core.messages.Message;
import arhangel.dim.core.messages.RegisterMessage;
import arhangel.dim.core.messages.StatusMessage;
import arhangel.dim.core.messages.TextMessage;
import arhangel.dim.core.messages.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Простейший протокол передачи данных
 */
public class StringProtocol implements Protocol {

    static Logger log = LoggerFactory.getLogger(StringProtocol.class);

    public static final String DELIMITER = ";";

    @Override
    public Message decode(byte[] bytes) throws ProtocolException {
        String str = new String(bytes);
        log.info("decoded: {}", str);
        String[] tokens = str.split(DELIMITER);
        Type type = Type.valueOf(tokens[0]);
        switch (type) {
            case MSG_REGISTER:
                RegisterMessage registerMessage = new RegisterMessage();
                registerMessage.setType(type);
                registerMessage.setSenderId(parseLong(tokens[1]));
                registerMessage.setLogin(tokens[2]);
                registerMessage.setSecret(tokens[3]);
                return registerMessage;
            case MSG_TEXT:
                TextMessage textMsg = new TextMessage();
                textMsg.setSenderId(parseLong(tokens[1]));
                textMsg.setChatId(parseLong(tokens[2]));
                textMsg.setText(tokens[3]);
                return textMsg;
            case MSG_INFO:
                InfoMessage infoMessage = new InfoMessage();
                infoMessage.setType(type);
                infoMessage.setSenderId(parseLong(tokens[1]));
                infoMessage.setUserId(parseLong(tokens[2]));

                return infoMessage;
            case MSG_STATUS:
                StatusMessage statusMessage = new StatusMessage();
                statusMessage.setSenderId(parseLong(tokens[1]));
                statusMessage.setStatus(tokens[2]);
                return statusMessage;
            case MSG_LOGIN:
                LoginMessage loginMessage = new LoginMessage();
                loginMessage.setSenderId(parseLong(tokens[1]));
                loginMessage.setLogin(tokens[2]);
                loginMessage.setPassword(tokens[3]);
                return loginMessage;
            case MSG_CHAT_LIST:
                ListChatMessage listChatMessage = new ListChatMessage();
                listChatMessage.setSenderId(parseLong(tokens[1]));
                return listChatMessage;
            case MSG_CHAT_CREATE:
                CreateChatMessage chatCreateMessage = new CreateChatMessage();
                chatCreateMessage.setSenderId(parseLong(tokens[1]));
                String[] userIdsStr = tokens[2].split(",");
                List<Long> userIds = new ArrayList<Long>();
                for (int i = 0; i < userIdsStr.length; ++i) {
                    userIds.add(Long.parseLong(userIdsStr[i]));
                }
                chatCreateMessage.setUsersIds(userIds);
                return chatCreateMessage;
            case MSG_CHAT_LIST_RESULT:
                ListChatResultMessage chatListResultMessage = new ListChatResultMessage();
                chatListResultMessage.setSenderId(parseLong(tokens[1]));
                if (tokens.length >= 3) {
                    chatListResultMessage.setChatIds(Arrays.asList(tokens[2].split(",")).stream()
                            .map(this::parseLong)
                            .collect(Collectors.toList()));
                }
                return chatListResultMessage;
            case MSG_CHAT_HIST_RESULT:
                HistChatResultMessage chatHistResultMessage = new HistChatResultMessage();
                chatHistResultMessage.setSenderId(parseLong(tokens[1]));
                if (tokens.length >= 3) {
                    chatHistResultMessage.setHistory(tokens[3]);
                }
                return chatHistResultMessage;
            case MSG_CHAT_HIST:
                HistChatMessage histChatMessage = new HistChatMessage();
                histChatMessage.setSenderId(parseLong(tokens[1]));
                histChatMessage.setChatId(parseLong(tokens[2]));
                histChatMessage.setType(type);
                return histChatMessage;
            default:
                throw new ProtocolException("Invalid type: " + type);
        }
    }

    @Override
    public byte[] encode(Message msg) throws ProtocolException {
        StringBuilder builder = new StringBuilder();
        Type type = msg.getType();
        builder.append(type).append(DELIMITER);
        switch (type) {
            case MSG_REGISTER:
                RegisterMessage registerMessage = (RegisterMessage) msg;
                builder.append(String.valueOf(registerMessage.getSenderId())).append(DELIMITER);
                builder.append(registerMessage.getLogin()).append(DELIMITER);
                builder.append(registerMessage.getSecret()).append(DELIMITER);
                break;
            case MSG_TEXT:
                TextMessage sendMessage = (TextMessage) msg;
                builder.append(String.valueOf(sendMessage.getSenderId())).append(DELIMITER);
                builder.append(sendMessage.getChatId()).append(DELIMITER);
                builder.append(sendMessage.getText()).append(DELIMITER);
                break;
            case MSG_LOGIN:
                LoginMessage loginMessage = (LoginMessage) msg;
                builder.append(String.valueOf(loginMessage.getSenderId())).append(DELIMITER);
                builder.append(loginMessage.getLogin()).append(DELIMITER);
                builder.append(loginMessage.getPassword()).append(DELIMITER);
                break;
            case MSG_CHAT_CREATE:
                CreateChatMessage chatCreateMessage = (CreateChatMessage) msg;
                List<Long> userIds = chatCreateMessage.getUsersIds();
                List<String> userIdsStr = new ArrayList<String>();
                for (int i = 0; i < userIds.size(); ++i) {
                    userIdsStr.add(String.valueOf(userIds.get(i)));
                }
                builder.append(String.valueOf(chatCreateMessage.getSenderId())).append(DELIMITER);
                builder.append(String.join(",",
                        userIdsStr)).append(DELIMITER);
                break;
            case MSG_STATUS:
                StatusMessage statusMessage = (StatusMessage) msg;
                builder.append(String.valueOf(statusMessage.getSenderId())).append(DELIMITER);
                builder.append(statusMessage.getStatus()).append(DELIMITER);
                break;
            case MSG_CHAT_LIST_RESULT:
                ListChatResultMessage listChatResultMessage = (ListChatResultMessage) msg;
                builder.append(String.valueOf(listChatResultMessage.getSenderId())).append(DELIMITER);
                builder.append(String.join(",",
                        listChatResultMessage.getChatIds().stream()
                                .map(Object::toString)
                                .collect(Collectors.toList()))).append(DELIMITER);
                break;
            case MSG_INFO:
                InfoMessage infoMessage = (InfoMessage) msg;
                builder.append(String.valueOf(infoMessage.getSenderId())).append(DELIMITER);
                builder.append(String.valueOf(infoMessage.getUserId())).append(DELIMITER);
                builder.append(infoMessage.getInfo()).append(DELIMITER);
                break;
            case MSG_CHAT_LIST:
                ListChatMessage listChatMessage = (ListChatMessage) msg;
                builder.append(String.valueOf(listChatMessage.getSenderId())).append(DELIMITER);
                builder.append(listChatMessage.getChatsList()).append(DELIMITER);
                break;
            case MSG_CHAT_HIST:
                HistChatMessage histChatMessage = (HistChatMessage) msg;
                builder.append(String.valueOf(histChatMessage.getSenderId())).append(DELIMITER);
                builder.append(String.valueOf(histChatMessage.getChatId())).append(DELIMITER);
                break;
            case MSG_CHAT_HIST_RESULT:
                HistChatResultMessage histChatResultMessage = (HistChatResultMessage) msg;
                builder.append(String.valueOf(histChatResultMessage.getSenderId())).append(DELIMITER);
                builder.append(String.valueOf(histChatResultMessage.getChatId())).append(DELIMITER);
                builder.append(histChatResultMessage.getHistory()).append(DELIMITER);
                break;
            default:
                throw new ProtocolException("Invalid type: " + type);


        }
        log.info("encoded: {}", builder.toString());
        return builder.toString().getBytes();
    }

    private Long parseLong(String str) {
        try {
            return Long.parseLong(str);
        } catch (Exception e) {
            // who care
        }
        return null;
    }
}