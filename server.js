const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const path = require("path");
const fs = require("fs");

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    },
    maxHttpBufferSize: 50 * 1024 * 1024
});

const PORT = process.env.PORT || 3000;

const PUBLIC_DIR = path.join(__dirname, "public");
const DATA_FILE = path.join(__dirname, "data.json");

app.use(express.static(PUBLIC_DIR));
app.use(express.json({ limit: "50mb" }));

let database = {
    messages: {},
    users: {}
};

function loadDatabase() {
    try {
        if (fs.existsSync(DATA_FILE)) {
            const data = fs.readFileSync(DATA_FILE, "utf8");
            if (data.trim()) {
                database = JSON.parse(data);
                Object.values(database.users).forEach(u => u.voiceChannel = null);
            }
        }
    } catch (error) {
        console.error("Erro ao carregar banco:", error);
        database = { messages: {}, users: {} };
    }
}

function saveDatabase() {
    fs.writeFile(DATA_FILE, JSON.stringify(database, null, 2), "utf8", (error) => {
        if (error) {
            console.error("Erro ao salvar banco:", error);
        }
    });
}

loadDatabase();

function getChannelMessages(channelKey) {
    if (!database.messages[channelKey]) {
        database.messages[channelKey] = [];
    }
    return database.messages[channelKey];
}

function cleanUser(user) {
    return {
        id: user.id,
        name: user.name || "Usuário",
        avatar: user.avatar || "",
        status: user.status || "Online",
        banner: user.banner || "color:#3b82f6",
        voiceChannel: user.voiceChannel || null
    };
}

function getOnlineUsers() {
    return Object.values(database.users).map(cleanUser);
}

function broadcastUsers() {
    io.emit("users_updated", getOnlineUsers());
}

io.on("connection", (socket) => {
    console.log("🟢 Usuário conectado:", socket.id);

    database.users[socket.id] = {
        id: socket.id,
        name: "Usuário",
        avatar: "",
        status: "Online",
        banner: "color:#3b82f6",
        voiceChannel: null
    };

    socket.emit("connection_ready", { id: socket.id });
    broadcastUsers();

    socket.on("update_user", (data = {}) => {
        if (!database.users[socket.id]) return;
        database.users[socket.id].name = String(data.name || "Usuário").substring(0, 50);
        database.users[socket.id].avatar = String(data.avatar || "");
        database.users[socket.id].status = String(data.status || "Online").substring(0, 100);
        database.users[socket.id].banner = String(data.banner || "color:#3b82f6");
        broadcastUsers();
    });

    socket.on("join_channel", (channelKey) => {
        if (!channelKey) return;
        socket.join(channelKey);
        const messages = getChannelMessages(channelKey);
        socket.emit("channel_history", { channelKey, messages });
    });

    socket.on("send_message", (data = {}) => {
        if (!data.channelKey) return;
        const channelKey = data.channelKey;

        const message = {
            id: Number(data.id) || Date.now(),
            server: data.server || "servidor1",
            channel: data.channel || "geral",
            channelKey,
            authorId: socket.id,
            author: String(data.author || "Usuário").substring(0, 50),
            avatar: data.avatar || "",
            banner: data.banner || "color:#3b82f6",
            status: data.status || "Online",
            time: data.time || new Date().toLocaleTimeString("pt-BR", { hour: "2-digit", minute: "2-digit" }),
            text: String(data.text || "").substring(0, 5000),
            image: data.image || null,
            sticker: data.sticker || null,
            pinned: false,
            createdAt: Date.now()
        };

        const messages = getChannelMessages(channelKey);
        messages.push(message);

        if (messages.length > 1000) {
            messages.splice(0, messages.length - 1000);
        }

        saveDatabase();
        io.to(channelKey).emit("receive_message", message);
    });

    socket.on("toggle_pin", (data = {}) => {
        if (!data.channelKey || !data.id) return;
        const messages = getChannelMessages(data.channelKey);
        const message = messages.find(m => Number(m.id) === Number(data.id));
        if (!message) return;

        message.pinned = Boolean(data.pinned);
        saveDatabase();

        io.to(data.channelKey).emit("message_pinned_toggled", {
            id: message.id,
            pinned: message.pinned,
            server: message.server,
            channel: message.channel
        });
    });

    socket.on("delete_message", (data = {}) => {
        if (!data.channelKey || !data.id) return;
        const messages = getChannelMessages(data.channelKey);
        const index = messages.findIndex(m => Number(m.id) === Number(data.id));
        if (index === -1) return;

        const message = messages[index];
        if (message.authorId && message.authorId !== socket.id) return; 

        messages.splice(index, 1);
        saveDatabase();

        io.to(data.channelKey).emit("message_deleted", {
            id: Number(data.id),
            server: data.server,
            channel: data.channel,
            channelKey: data.channelKey
        });
    });

    socket.on("join_voice", (room) => {
        if (!room) return;
        const user = database.users[socket.id];

        if (user && user.voiceChannel) {
            socket.leave(user.voiceChannel);
            socket.to(user.voiceChannel).emit("user_left_voice", socket.id);
        }

        if (user) user.voiceChannel = room;
        socket.join(room);

        const roomSockets = io.sockets.adapter.rooms.get(room);
        if (roomSockets) {
            for (const userId of roomSockets) {
                if (userId === socket.id) continue;
                socket.emit("voice_user_present", userId);
            }
        }

        socket.to(room).emit("user_joined_voice", socket.id);
        saveDatabase();
        broadcastUsers();
    });

    socket.on("leave_voice", (room) => {
        if (!room) return;
        socket.leave(room);
        if (database.users[socket.id]) database.users[socket.id].voiceChannel = null;
        socket.to(room).emit("user_left_voice", socket.id);
        saveDatabase();
        broadcastUsers();
    });

    socket.on("voice_offer", (data = {}) => {
        if (!data.target || !data.offer) return;
        io.to(data.target).emit("voice_offer", { offer: data.offer, sender: socket.id });
    });

    socket.on("voice_answer", (data = {}) => {
        if (!data.target || !data.answer) return;
        io.to(data.target).emit("voice_answer", { answer: data.answer, sender: socket.id });
    });

    socket.on("ice_candidate", (data = {}) => {
        if (!data.target || !data.candidate) return;
        io.to(data.target).emit("ice_candidate", { candidate: data.candidate, sender: socket.id });
    });

    socket.on("disconnect", () => {
        const user = database.users[socket.id];
        if (user && user.voiceChannel) {
            socket.to(user.voiceChannel).emit("user_left_voice", socket.id);
        }
        delete database.users[socket.id];
        saveDatabase();
        broadcastUsers();
    });
});
app.get("*", (req, res) => {
    res.sendFile(path.join(PUBLIC_DIR, "index.html"));
});
server.listen(PORT, () => {
    console.log(`Servidor rodando na porta ${PORT}`);
});
