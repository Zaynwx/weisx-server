const express = require("express");
const http = require("http");
const crypto = require("crypto");
const util = require("util");
const { Server } = require("socket.io");
const cors = require("cors");
const { Pool } = require("pg");

const scrypt = util.promisify(crypto.scrypt);

if (!process.env.DATABASE_URL) {
    console.error("ERRO: a variável DATABASE_URL não está configurada no Render.");
    process.exit(1);
}

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false },
    max: 5
});

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*" }
});

// Salas ficam em memória (não precisam sobreviver a reinícios)
const rooms = new Map(); // roomCode -> { hostId, participants: Map, viewers: Map }

function viewersOf(room, broadcasterId) {
    if (!room.viewers) room.viewers = new Map();
    if (!room.viewers.has(broadcasterId)) room.viewers.set(broadcasterId, new Set());
    return room.viewers.get(broadcasterId);
}

// ---------- Servidores (grupos), canais, membros e chat ----------
let mainServerId = null;
const channelsByServer = new Map(); // serverId -> [{ id, name }]
const channelIndex = new Map();     // channelId -> serverId
const categoriesByServer = new Map(); // serverId -> [{ id, name }]
const categoryIndex = new Map();    // categoryId -> serverId
const userSockets = new Map();      // userId -> Set(socket)
const ROLE_RANK = { owner: 4, admin: 3, moderator: 2, member: 1 };
const MAX_IMAGE_CHARS = 60000;

function validImage(value) {
    return typeof value === "string" && value.length <= MAX_IMAGE_CHARS &&
        /^data:image\/(png|jpeg|webp);base64,[A-Za-z0-9+\/=]+$/.test(value);
}
const joinFailures = new Map();     // "userId:serverId" -> { count, until }

async function loadChannels() {
    const result = await pool.query("SELECT id, server_id, name, category_id FROM channels ORDER BY position, id");
    channelsByServer.clear();
    channelIndex.clear();
    for (const row of result.rows) {
        if (!channelsByServer.has(row.server_id)) channelsByServer.set(row.server_id, []);
        channelsByServer.get(row.server_id).push({ id: row.id, name: row.name, categoryId: row.category_id || null });
        channelIndex.set(row.id, row.server_id);
    }
    const cats = await pool.query("SELECT id, server_id, name FROM categories ORDER BY position, id");
    categoriesByServer.clear();
    categoryIndex.clear();
    for (const row of cats.rows) {
        if (!categoriesByServer.has(row.server_id)) categoriesByServer.set(row.server_id, []);
        categoriesByServer.get(row.server_id).push({ id: row.id, name: row.name });
        categoryIndex.set(row.id, row.server_id);
    }
}

function channelsState(serverId) {
    return (channelsByServer.get(serverId) || []).map(channel => {
        const room = rooms.get("ch:" + channel.id);
        const users = room
            ? Array.from(room.participants.values()).map(p => ({ id: p.id, name: p.name, broadcasting: !!p.broadcasting, avatar: p.avatar || null }))
            : [];
        return { id: channel.id, name: channel.name, categoryId: channel.categoryId || null, users };
    });
}

function broadcastServer(serverId) {
    if (!serverId) return;
    io.to("srv:" + serverId).emit("server-state", {
        serverId,
        channels: channelsState(serverId),
        categories: categoriesByServer.get(serverId) || []
    });
}

// Garante que o usuário é membro do servidor WEISX e devolve o cargo dele
async function ensureMainMembership(user) {
    const server = (await pool.query("SELECT id, owner_id FROM servers WHERE id = $1", [mainServerId])).rows[0];

    if (!server.owner_id) {
        const wanted = (process.env.OWNER_USERNAME || "").trim().toLowerCase();
        const ownerRow = wanted
            ? (await pool.query("SELECT id FROM users WHERE username_lower = $1", [wanted])).rows[0]
            : (await pool.query("SELECT id FROM users ORDER BY id LIMIT 1")).rows[0];
        if (ownerRow) {
            await pool.query("UPDATE servers SET owner_id = $1 WHERE id = $2 AND owner_id IS NULL", [ownerRow.id, mainServerId]);
            await pool.query(
                `INSERT INTO server_members (server_id, user_id, role) VALUES ($1, $2, 'owner')
                 ON CONFLICT (server_id, user_id) DO UPDATE SET role = 'owner'`,
                [mainServerId, ownerRow.id]
            );
        }
    }

    await pool.query(
        "INSERT INTO server_members (server_id, user_id, role) VALUES ($1, $2, 'member') ON CONFLICT (server_id, user_id) DO NOTHING",
        [mainServerId, user.id]
    );
    const member = (await pool.query(
        "SELECT role FROM server_members WHERE server_id = $1 AND user_id = $2", [mainServerId, user.id]
    )).rows[0];
    return member ? member.role : "member";
}

async function getRole(userId, serverId) {
    const result = await pool.query(
        "SELECT role FROM server_members WHERE server_id = $1 AND user_id = $2",
        [serverId, userId]
    );
    return result.rows[0] ? result.rows[0].role : null;
}

async function requireManager(socket, serverId) {
    if (!socket.data.user || !serverId) return false;
    const role = await getRole(socket.data.user.id, serverId);
    return role === "owner" || role === "admin";
}

async function listMemberships(userId) {
    const result = await pool.query(
        `SELECT s.id, s.name, s.is_main, (s.password_hash IS NOT NULL) AS has_password, s.icon, m.role
         FROM server_members m JOIN servers s ON s.id = m.server_id
         WHERE m.user_id = $1 ORDER BY s.is_main DESC, s.id`,
        [userId]
    );
    return result.rows;
}

function serverPayload(row) {
    return {
        id: row.id,
        name: row.name,
        isMain: !!row.is_main,
        hasPassword: !!row.has_password,
        role: row.role,
        icon: row.icon || null,
        channels: channelsState(row.id),
        categories: categoriesByServer.get(row.id) || []
    };
}

function cleanServerName(value) {
    const name = String(value || "").replace(/[\u0000-\u001f\u007f]/g, "").replace(/\s+/g, " ").trim();
    return name.length >= 2 && name.length <= 32 ? name : null;
}

async function loadMessages(channelId) {
    const result = await pool.query(
        "SELECT id, username, content, kind, created_at FROM messages WHERE channel_id = $1 ORDER BY id DESC LIMIT 50",
        [channelId]
    );
    return result.rows.reverse().map(row => ({
        id: row.id, kind: row.kind, username: row.username, text: row.content, at: row.created_at
    }));
}

async function pruneMessages(channelId) {
    await pool.query(
        `DELETE FROM messages WHERE channel_id = $1 AND id <= (
            SELECT id FROM messages WHERE channel_id = $1 ORDER BY id DESC OFFSET 500 LIMIT 1
         )`,
        [channelId]
    );
}

// Linha de sistema no chat do canal ("fulano entrou" / "fulano saiu")
async function postSystem(channelId, username, kind) {
    const saved = await pool.query(
        "INSERT INTO messages (channel_id, username, content, kind) VALUES ($1, $2, '', $3) RETURNING id, created_at",
        [channelId, username, kind]
    );
    await pruneMessages(channelId);
    io.to("ch:" + channelId).emit("chat-message", {
        channelId, id: saved.rows[0].id, kind, username, text: "", at: saved.rows[0].created_at
    });
}

// ---------- Banco de dados (contas e sessões) ----------
const SESSION_DAYS = 90;

async function initDb() {
    await pool.query(`
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username TEXT NOT NULL,
            username_lower TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            weisx_id TEXT NOT NULL UNIQUE,
            recovery_email TEXT,
            recovery_email_verified BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);
    await pool.query(`
        CREATE TABLE IF NOT EXISTS sessions (
            token TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);
    await pool.query(`DELETE FROM sessions WHERE created_at < NOW() - INTERVAL '${SESSION_DAYS} days'`);

    await pool.query(`
        CREATE TABLE IF NOT EXISTS servers (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            owner_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
            password_hash TEXT,
            is_main BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);
    await pool.query(`
        CREATE TABLE IF NOT EXISTS channels (
            id SERIAL PRIMARY KEY,
            server_id INTEGER NOT NULL REFERENCES servers(id) ON DELETE CASCADE,
            name TEXT NOT NULL,
            position INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);
    await pool.query(`
        CREATE TABLE IF NOT EXISTS server_members (
            server_id INTEGER NOT NULL REFERENCES servers(id) ON DELETE CASCADE,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            role TEXT NOT NULL DEFAULT 'member',
            PRIMARY KEY (server_id, user_id)
        )
    `);
    await pool.query(`
        CREATE TABLE IF NOT EXISTS messages (
            id BIGSERIAL PRIMARY KEY,
            channel_id INTEGER NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
            user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
            username TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);

    await pool.query("ALTER TABLE messages ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'msg'");
    await pool.query("CREATE UNIQUE INDEX IF NOT EXISTS servers_name_lower ON servers (LOWER(name))");
    await pool.query("ALTER TABLE users ADD COLUMN IF NOT EXISTS avatar TEXT");
    await pool.query("ALTER TABLE servers ADD COLUMN IF NOT EXISTS icon TEXT");
    await pool.query(`
        CREATE TABLE IF NOT EXISTS categories (
            id SERIAL PRIMARY KEY,
            server_id INTEGER NOT NULL REFERENCES servers(id) ON DELETE CASCADE,
            name TEXT NOT NULL,
            position INTEGER NOT NULL DEFAULT 0
        )
    `);
    await pool.query("ALTER TABLE channels ADD COLUMN IF NOT EXISTS category_id INTEGER REFERENCES categories(id) ON DELETE SET NULL");

    let main = (await pool.query("SELECT id FROM servers WHERE is_main LIMIT 1")).rows[0];
    if (!main) {
        main = (await pool.query("INSERT INTO servers (name, is_main) VALUES ('WEISX', TRUE) RETURNING id")).rows[0];
    }
    mainServerId = main.id;

    const count = (await pool.query("SELECT COUNT(*)::int AS n FROM channels WHERE server_id = $1", [mainServerId])).rows[0].n;
    if (count === 0) {
        const defaults = ["Geral", "Resenha", "Duo", "Trio", "Squad"];
        for (let i = 0; i < defaults.length; i++) {
            await pool.query("INSERT INTO channels (server_id, name, position) VALUES ($1, $2, $3)", [mainServerId, defaults[i], i + 1]);
        }
    }
    await loadChannels();
}

async function hashPassword(password) {
    const salt = crypto.randomBytes(16).toString("hex");
    const hash = (await scrypt(password, salt, 64)).toString("hex");
    return salt + ":" + hash;
}

async function verifyPassword(password, stored) {
    const [salt, hash] = String(stored).split(":");
    if (!salt || !hash) return false;
    const test = await scrypt(password, salt, 64);
    const expected = Buffer.from(hash, "hex");
    return test.length === expected.length && crypto.timingSafeEqual(test, expected);
}

function publicUser(row) {
    return {
        username: row.username,
        weisxId: row.weisx_id,
        recoveryEmail: row.recovery_email,
        recoveryEmailVerified: row.recovery_email_verified
    };
}

function readToken(req) {
    const header = req.headers.authorization;
    if (!header) return null;
    return header.split(" ")[1] || null;
}

async function getUserByToken(token) {
    if (!token || typeof token !== "string") return null;
    const result = await pool.query(
        `SELECT u.* FROM sessions s JOIN users u ON u.id = s.user_id
         WHERE s.token = $1 AND s.created_at > NOW() - INTERVAL '${SESSION_DAYS} days'`,
        [token]
    );
    return result.rows[0] || null;
}

function serverError(res, error) {
    console.error(error);
    res.status(500).json({ success: false, message: "Erro no servidor. Tente novamente." });
}

// Rota de Registo
app.post("/api/auth/register", async (req, res) => {
    try {
        const { username, password } = req.body || {};
        if (!username || !password) {
            return res.status(400).json({ success: false, message: "Preencha todos os campos." });
        }
        const cleanName = String(username).trim();
        if (!cleanName || cleanName.length > 32 || String(password).length > 128) {
            return res.status(400).json({ success: false, message: "Nome de usuário ou senha em formato inválido." });
        }

        let weisxId;
        for (let i = 0; i < 10; i++) {
            const candidate = "W-" + Math.floor(100000 + Math.random() * 900000);
            const taken = await pool.query("SELECT 1 FROM users WHERE weisx_id = $1", [candidate]);
            if (taken.rowCount === 0) { weisxId = candidate; break; }
        }
        if (!weisxId) return serverError(res, new Error("Sem ID WEISX livre"));

        const passwordHash = await hashPassword(String(password));
        const result = await pool.query(
            `INSERT INTO users (username, username_lower, password_hash, weisx_id)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (username_lower) DO NOTHING
             RETURNING *`,
            [cleanName, cleanName.toLowerCase(), passwordHash, weisxId]
        );
        if (result.rowCount === 0) {
            return res.status(400).json({ success: false, message: "Este nome de usuário já está em uso." });
        }

        const user = result.rows[0];
        res.json({ success: true, user: { username: user.username, weisxId: user.weisx_id } });
    } catch (error) {
        serverError(res, error);
    }
});

// Rota de Login
app.post("/api/auth/login", async (req, res) => {
    try {
        const { identifier, password } = req.body || {};
        if (!identifier || !password) {
            return res.status(400).json({ success: false, message: "Preencha a conta e a senha." });
        }

        const cleanId = String(identifier).trim().toLowerCase();
        const found = await pool.query(
            "SELECT * FROM users WHERE username_lower = $1 OR LOWER(weisx_id) = $1 LIMIT 1",
            [cleanId]
        );
        const user = found.rows[0];

        if (!user || !(await verifyPassword(String(password), user.password_hash))) {
            return res.status(401).json({ success: false, message: "Usuário ou senha incorretos." });
        }

        const token = crypto.randomBytes(32).toString("hex");
        await pool.query("INSERT INTO sessions (token, user_id) VALUES ($1, $2)", [token, user.id]);

        res.json({ success: true, token, user: publicUser(user) });
    } catch (error) {
        serverError(res, error);
    }
});

// Rota /me (Verificar sessão)
app.get("/api/auth/me", async (req, res) => {
    try {
        const user = await getUserByToken(readToken(req));
        if (!user) return res.status(401).json({ success: false });
        res.json({ success: true, user: publicUser(user) });
    } catch (error) {
        serverError(res, error);
    }
});

app.post("/api/auth/logout", async (req, res) => {
    try {
        const token = readToken(req);
        if (token) await pool.query("DELETE FROM sessions WHERE token = $1", [token]);
        res.json({ success: true });
    } catch (error) {
        serverError(res, error);
    }
});

// Perfil
app.get("/api/account/profile", async (req, res) => {
    try {
        const user = await getUserByToken(readToken(req));
        if (!user) return res.status(401).json({ success: false, message: "Sessão expirada." });
        res.json({ success: true, user: publicUser(user) });
    } catch (error) {
        serverError(res, error);
    }
});

// Descobre o nome da conta a partir do token de login (ou null se inválido)
async function resolveUsernameFromToken(token) {
    try {
        const user = await getUserByToken(token);
        return user ? user.username : null;
    } catch (error) {
        console.error("Erro ao validar token do socket:", error);
        return null;
    }
}

// WebSocket para Salas e Transmissão
io.on("connection", socket => {
    let currentRoomCode = null;
    let currentUsername = "Convidado";

    socket.on("create-room", async (roomCode, auth) => {
        currentUsername = (await resolveUsernameFromToken(auth && auth.token)) || "Convidado";
        currentRoomCode = roomCode;
        socket.join(roomCode);
        rooms.set(roomCode, {
            hostId: socket.id,
            participants: new Map([[socket.id, { id: socket.id, name: currentUsername, isHost: true, broadcasting: false }]])
        });
        socket.emit("room-created", { selfId: socket.id, roomCode });
        updateRoomState(roomCode);
    });

    socket.on("join-room", async (roomCode, auth) => {
        const room = rooms.get(roomCode);
        if (!room) {
            socket.emit("room-error", "Sala não encontrada.");
            return;
        }
        currentUsername = (await resolveUsernameFromToken(auth && auth.token)) || "Convidado";
        currentRoomCode = roomCode;
        socket.join(roomCode);
        room.participants.set(socket.id, { id: socket.id, name: currentUsername, isHost: false, broadcasting: false });
        socket.emit("room-joined", { selfId: socket.id, roomCode });
        updateRoomState(roomCode);
    });

    socket.on("start-broadcast", data => {
        if (!currentRoomCode) return;
        const room = rooms.get(currentRoomCode);
        if (!room) return;
        const p = room.participants.get(socket.id);
        if (p) {
            p.broadcasting = true;
            p.viewerLimit = Math.max(0, Math.min(50, parseInt(data && data.viewerLimit, 10) || 0));
            viewersOf(room, socket.id).clear();
            socket.emit("broadcast-started-self");
            updateRoomState(currentRoomCode);
        }
    });

    socket.on("stop-broadcast", () => {
        if (!currentRoomCode) return;
        const room = rooms.get(currentRoomCode);
        if (!room) return;
        const p = room.participants.get(socket.id);
        if (p) {
            p.broadcasting = false;
            p.viewerLimit = 0;
            viewersOf(room, socket.id).clear();
            updateRoomState(currentRoomCode);
            io.to(currentRoomCode).emit("participant-broadcast-stopped", { participantId: socket.id });
        }
    });

    socket.on("watch-stream", data => {
        if (!data || !data.broadcasterId) return;
        const room = currentRoomCode ? rooms.get(currentRoomCode) : null;
        const target = room ? room.participants.get(data.broadcasterId) : null;

        if (room && target) {
            const viewers = viewersOf(room, data.broadcasterId);
            if (target.viewerLimit > 0 && !viewers.has(socket.id) && viewers.size >= target.viewerLimit) {
                socket.emit("watch-error", {
                    broadcasterId: data.broadcasterId,
                    message: "Esta transmissão atingiu o limite de espectadores."
                });
                return;
            }
            viewers.add(socket.id);
        }

        io.to(data.broadcasterId).emit("viewer-request", { viewerId: socket.id });
    });

    socket.on("stop-watching", data => {
        if (!data || !data.broadcasterId) return;
        const room = currentRoomCode ? rooms.get(currentRoomCode) : null;
        if (room) viewersOf(room, data.broadcasterId).delete(socket.id);
        io.to(data.broadcasterId).emit("viewer-left", { viewerId: socket.id });
    });

    socket.on("signal", data => {
        if (!data || !data.targetId) return;
        io.to(data.targetId).emit("signal", {
            sourceId: socket.id,
            type: data.type,
            sdp: data.sdp,
            candidate: data.candidate,
            connectionId: data.connectionId
        });
    });

    function leaveCurrentRoom() {
        if (!currentRoomCode) return;
        const code = currentRoomCode;
        currentRoomCode = null;
        socket.data.currentRoom = null;
        socket.leave(code);

        const room = rooms.get(code);
        if (!room) return;

        if (room.hostId === socket.id) {
            io.to(code).emit("host-left");
            rooms.delete(code);
        } else {
            const who = room.participants.get(socket.id);
            room.participants.delete(socket.id);
            if (room.channelId && who) {
                postSystem(room.channelId, who.name, "leave").catch(error => console.error("Erro no aviso de saída:", error));
            }
            if (room.viewers) {
                room.viewers.delete(socket.id);
                room.viewers.forEach(set => set.delete(socket.id));
            }
            if (room.channelId && room.participants.size === 0) {
                rooms.delete(code);
                broadcastServer(room.serverId);
            } else {
                updateRoomState(code);
            }
            io.to(code).emit("participant-left", { participantId: socket.id });
        }
    }

    // ----- Servidor WEISX: canais permanentes e chat -----
    socket.on("open-workspace", async data => {
        try {
            const user = await getUserByToken(data && data.token);
            if (!user) {
                socket.emit("workspace-error", "Sessão inválida. Entre novamente.");
                return;
            }
            socket.data.user = { id: user.id, username: user.username, avatar: user.avatar || null };
            socket.data.roles = new Map();
            registerUserSocket();
            await ensureMainMembership(user);
            const memberships = await listMemberships(user.id);
            for (const row of memberships) {
                socket.data.roles.set(row.id, row.role);
                socket.join("srv:" + row.id);
            }
            socket.emit("workspace-ready", { servers: memberships.map(serverPayload), me: { avatar: user.avatar || null } });
        } catch (error) {
            console.error("Erro ao abrir a área de trabalho:", error);
            socket.emit("workspace-error", "Não foi possível carregar os grupos. Tente novamente.");
        }
    });

    socket.on("close-workspace", () => {
        unregisterUserSocket();
        leaveCurrentRoom();
        if (socket.data.roles) socket.data.roles.forEach((role, id) => socket.leave("srv:" + id));
        socket.data.roles = null;
        socket.data.user = null;
    });

    socket.on("join-channel", async data => {
        try {
            if (!socket.data.user) {
                socket.emit("room-error", "Sessão inválida. Entre novamente.");
                return;
            }
            const channelId = parseInt(data && data.channelId, 10);
            const serverId = channelIndex.get(channelId);
            const channel = serverId ? (channelsByServer.get(serverId) || []).find(c => c.id === channelId) : null;
            if (!channel) {
                socket.emit("room-error", "Canal não encontrado.");
                return;
            }
            if (!socket.data.roles || !socket.data.roles.has(serverId)) {
                socket.emit("room-error", "Você não faz parte deste grupo.");
                return;
            }

            leaveCurrentRoom();

            const code = "ch:" + channelId;
            let room = rooms.get(code);
            if (!room) {
                room = { hostId: null, channelId, serverId, participants: new Map(), viewers: new Map() };
                rooms.set(code, room);
            }
            currentRoomCode = code;
            socket.data.currentRoom = code;
            currentUsername = socket.data.user.username;
            socket.join(code);
            room.participants.set(socket.id, { id: socket.id, name: currentUsername, isHost: false, broadcasting: false, avatar: socket.data.user.avatar || null });

            socket.emit("room-joined", { selfId: socket.id, roomCode: code, channelId, channelName: channel.name });
            const avatarRows = (await pool.query(
                "SELECT username, avatar FROM users WHERE avatar IS NOT NULL AND username IN (SELECT DISTINCT username FROM messages WHERE channel_id = $1)",
                [channelId]
            )).rows;
            const avatars = {};
            for (const row of avatarRows) avatars[row.username] = row.avatar;
            socket.emit("chat-history", { channelId, messages: await loadMessages(channelId), avatars });
            updateRoomState(code);
            await postSystem(channelId, currentUsername, "join");
        } catch (error) {
            console.error("Erro ao entrar no canal:", error);
            socket.emit("room-error", "Não foi possível entrar no canal.");
        }
    });

    socket.on("chat-send", async data => {
        try {
            const room = currentRoomCode ? rooms.get(currentRoomCode) : null;
            if (!room || !room.channelId || !socket.data.user) return;
            const text = String((data && data.text) || "").trim().slice(0, 500);
            if (!text) return;

            const now = Date.now();
            if (now - (socket.data.lastChat || 0) < 300) return;
            socket.data.lastChat = now;

            const saved = await pool.query(
                "INSERT INTO messages (channel_id, user_id, username, content) VALUES ($1, $2, $3, $4) RETURNING id, created_at",
                [room.channelId, socket.data.user.id, socket.data.user.username, text]
            );
            await pruneMessages(room.channelId);
            io.to(currentRoomCode).emit("chat-message", {
                channelId: room.channelId,
                kind: "msg",
                id: saved.rows[0].id,
                username: socket.data.user.username,
                text,
                at: saved.rows[0].created_at
            });
        } catch (error) {
            console.error("Erro ao enviar mensagem:", error);
        }
    });

    function serverError(where, message) {
        socket.emit("server-error", { where, message });
    }

    function registerUserSocket() {
        const id = socket.data.user.id;
        if (!userSockets.has(id)) userSockets.set(id, new Set());
        userSockets.get(id).add(socket);
        socket.data.leaveRoom = leaveCurrentRoom;
    }

    function unregisterUserSocket() {
        const user = socket.data.user;
        if (!user) return;
        const set = userSockets.get(user.id);
        if (set) {
            set.delete(socket);
            if (set.size === 0) userSockets.delete(user.id);
        }
    }

    async function actorRole(serverId) {
        if (!socket.data.user || !serverId) return null;
        return getRole(socket.data.user.id, serverId);
    }

    socket.on("channel-create", async data => {
        try {
            const serverId = parseInt(data && data.serverId, 10);
            if (!(await requireManager(socket, serverId))) {
                serverError("other", "Você não tem permissão para criar canais neste grupo.");
                return;
            }
            const name = String((data && data.name) || "").trim().slice(0, 32);
            if (!name) return;
            if ((channelsByServer.get(serverId) || []).length >= 30) {
                serverError("other", "Limite de 30 canais atingido.");
                return;
            }
            let categoryId = parseInt(data && data.categoryId, 10);
            if (!categoryId || categoryIndex.get(categoryId) !== serverId) categoryId = null;
            await pool.query(
                `INSERT INTO channels (server_id, name, position, category_id)
                 VALUES ($1, $2, COALESCE((SELECT MAX(position) FROM channels WHERE server_id = $1), 0) + 1, $3)`,
                [serverId, name, categoryId]
            );
            await loadChannels();
            broadcastServer(serverId);
        } catch (error) {
            console.error("Erro ao criar canal:", error);
            serverError("other", "Não foi possível criar o canal.");
        }
    });

    socket.on("channel-rename", async data => {
        try {
            const channelId = parseInt(data && data.channelId, 10);
            const serverId = channelIndex.get(channelId);
            if (!(await requireManager(socket, serverId))) {
                serverError("other", "Você não tem permissão para renomear canais.");
                return;
            }
            const name = String((data && data.name) || "").trim().slice(0, 32);
            if (!name) return;
            await pool.query("UPDATE channels SET name = $1 WHERE id = $2 AND server_id = $3", [name, channelId, serverId]);
            await loadChannels();
            broadcastServer(serverId);
        } catch (error) {
            console.error("Erro ao renomear canal:", error);
        }
    });

    socket.on("channel-delete", async data => {
        try {
            const channelId = parseInt(data && data.channelId, 10);
            const serverId = channelIndex.get(channelId);
            if (!(await requireManager(socket, serverId))) {
                serverError("other", "Você não tem permissão para excluir canais.");
                return;
            }
            const code = "ch:" + channelId;
            io.to(code).emit("channel-deleted", { channelId });
            io.in(code).socketsLeave(code);
            rooms.delete(code);

            await pool.query("DELETE FROM channels WHERE id = $1 AND server_id = $2", [channelId, serverId]);
            await loadChannels();
            broadcastServer(serverId);
        } catch (error) {
            console.error("Erro ao excluir canal:", error);
        }
    });

    // ----- Grupos próprios: criar, pesquisar pelo nome, entrar (com senha), sair, excluir -----
    socket.on("server-create", async data => {
        try {
            if (!socket.data.user) return serverError("create", "Sessão inválida. Entre novamente.");
            const name = cleanServerName(data && data.name);
            if (!name) return serverError("create", "O nome do grupo precisa ter de 2 a 32 caracteres.");
            const password = String((data && data.password) || "");
            if (password && (password.length < 4 || password.length > 64)) {
                return serverError("create", "A senha precisa ter de 4 a 64 caracteres.");
            }
            const owned = await pool.query(
                "SELECT COUNT(*)::int AS n FROM servers WHERE owner_id = $1 AND NOT is_main",
                [socket.data.user.id]
            );
            if (owned.rows[0].n >= 5) {
                return serverError("create", "Você já criou 5 grupos. Exclua um para criar outro.");
            }

            const hash = password ? await hashPassword(password) : null;
            let created;
            try {
                created = (await pool.query(
                    "INSERT INTO servers (name, owner_id, password_hash) VALUES ($1, $2, $3) RETURNING id",
                    [name, socket.data.user.id, hash]
                )).rows[0];
            } catch (error) {
                if (error && error.code === "23505") {
                    return serverError("create", "Já existe um grupo com esse nome. Escolha outro.");
                }
                throw error;
            }

            await pool.query("INSERT INTO server_members (server_id, user_id, role) VALUES ($1, $2, 'owner')", [created.id, socket.data.user.id]);
            await pool.query("INSERT INTO channels (server_id, name, position) VALUES ($1, 'Geral', 1)", [created.id]);
            await pool.query("INSERT INTO channels (server_id, name, position) VALUES ($1, 'Resenha', 2)", [created.id]);
            await loadChannels();

            socket.data.roles.set(created.id, "owner");
            socket.join("srv:" + created.id);
            socket.emit("server-added", serverPayload({ id: created.id, name, is_main: false, has_password: !!hash, role: "owner" }));
        } catch (error) {
            console.error("Erro ao criar grupo:", error);
            serverError("create", "Não foi possível criar o grupo. Tente novamente.");
        }
    });

    socket.on("server-search", async data => {
        try {
            if (!socket.data.user) return;
            const now = Date.now();
            if (now - (socket.data.lastSearch || 0) < 700) return;
            socket.data.lastSearch = now;

            const name = String((data && data.name) || "").trim().slice(0, 32);
            if (name.length < 2) return;
            const row = (await pool.query(
                `SELECT s.id, s.name, (s.password_hash IS NOT NULL) AS has_password,
                        (SELECT COUNT(*)::int FROM server_members WHERE server_id = s.id) AS members, s.icon
                 FROM servers s WHERE LOWER(s.name) = LOWER($1)`,
                [name]
            )).rows[0];

            if (!row) {
                socket.emit("server-search-result", { query: name, found: false });
                return;
            }
            socket.emit("server-search-result", {
                query: name,
                found: true,
                server: {
                    id: row.id,
                    name: row.name,
                    hasPassword: !!row.has_password,
                    members: row.members,
                    icon: row.icon || null,
                    isMember: socket.data.roles.has(row.id)
                }
            });
        } catch (error) {
            console.error("Erro na busca de grupo:", error);
        }
    });

    socket.on("server-join", async data => {
        try {
            if (!socket.data.user) return serverError("join", "Sessão inválida. Entre novamente.");
            const serverId = parseInt(data && data.serverId, 10);
            const found = (await pool.query(
                "SELECT id, name, is_main, password_hash FROM servers WHERE id = $1", [serverId]
            )).rows[0];
            if (!found) return serverError("join", "Grupo não encontrado.");

            const userId = socket.data.user.id;
            let role = await getRole(userId, serverId);

            if (!role) {
                if (found.password_hash) {
                    const key = userId + ":" + serverId;
                    const state = joinFailures.get(key) || { count: 0, until: 0 };
                    if (state.until > Date.now()) {
                        return serverError("join", "Muitas tentativas erradas. Espere um minuto e tente de novo.");
                    }
                    const ok = await verifyPassword(String((data && data.password) || ""), found.password_hash);
                    if (!ok) {
                        state.count += 1;
                        if (state.count >= 5) { state.until = Date.now() + 60000; state.count = 0; }
                        joinFailures.set(key, state);
                        return serverError("join", "Senha incorreta.");
                    }
                    joinFailures.delete(key);
                }
                await pool.query(
                    "INSERT INTO server_members (server_id, user_id, role) VALUES ($1, $2, 'member') ON CONFLICT (server_id, user_id) DO NOTHING",
                    [serverId, userId]
                );
                role = (await getRole(userId, serverId)) || "member";
            }

            socket.data.roles.set(serverId, role);
            socket.join("srv:" + serverId);
            socket.emit("server-added", serverPayload({
                id: found.id, name: found.name, is_main: found.is_main, has_password: !!found.password_hash, role
            }));
        } catch (error) {
            console.error("Erro ao entrar no grupo:", error);
            serverError("join", "Não foi possível entrar no grupo. Tente novamente.");
        }
    });

    socket.on("server-leave", async data => {
        try {
            if (!socket.data.user) return;
            const serverId = parseInt(data && data.serverId, 10);
            if (!serverId || serverId === mainServerId) return;
            const role = await getRole(socket.data.user.id, serverId);
            if (!role) return;
            if (role === "owner") {
                return serverError("other", "O dono não pode sair do próprio grupo. Exclua o grupo se não quiser mais.");
            }
            const current = currentRoomCode ? rooms.get(currentRoomCode) : null;
            if (current && current.serverId === serverId) leaveCurrentRoom();

            await pool.query("DELETE FROM server_members WHERE server_id = $1 AND user_id = $2", [serverId, socket.data.user.id]);
            socket.data.roles.delete(serverId);
            socket.leave("srv:" + serverId);
            socket.emit("server-removed", { serverId, reason: "left" });
        } catch (error) {
            console.error("Erro ao sair do grupo:", error);
        }
    });

    socket.on("server-delete", async data => {
        try {
            if (!socket.data.user) return;
            const serverId = parseInt(data && data.serverId, 10);
            if (!serverId || serverId === mainServerId) return;
            const role = await getRole(socket.data.user.id, serverId);
            if (role !== "owner") return serverError("other", "Só o dono pode excluir o grupo.");

            for (const channel of channelsByServer.get(serverId) || []) {
                const code = "ch:" + channel.id;
                io.in(code).socketsLeave(code);
                rooms.delete(code);
            }
            io.to("srv:" + serverId).emit("server-removed", { serverId, reason: "deleted" });
            io.in("srv:" + serverId).socketsLeave("srv:" + serverId);

            await pool.query("DELETE FROM servers WHERE id = $1", [serverId]);
            await loadChannels();
        } catch (error) {
            console.error("Erro ao excluir grupo:", error);
        }
    });

    socket.on("server-password", async data => {
        try {
            if (!socket.data.user) return;
            const serverId = parseInt(data && data.serverId, 10);
            if (!serverId || serverId === mainServerId) return;
            const role = await getRole(socket.data.user.id, serverId);
            if (role !== "owner") return serverError("other", "Só o dono pode mudar a senha do grupo.");
            const password = String((data && data.password) || "");
            if (password && (password.length < 4 || password.length > 64)) {
                return serverError("other", "A senha precisa ter de 4 a 64 caracteres.");
            }
            const hash = password ? await hashPassword(password) : null;
            await pool.query("UPDATE servers SET password_hash = $1 WHERE id = $2", [hash, serverId]);
            io.to("srv:" + serverId).emit("server-info", { serverId, hasPassword: !!hash });
        } catch (error) {
            console.error("Erro ao mudar a senha do grupo:", error);
        }
    });

    // ----- Categorias -----
    socket.on("category-create", async data => {
        try {
            const serverId = parseInt(data && data.serverId, 10);
            if (!(await requireManager(socket, serverId))) return serverError("other", "Você não tem permissão para criar categorias.");
            const name = String((data && data.name) || "").replace(/[\u0000-\u001f\u007f]/g, "").trim().slice(0, 32);
            if (!name) return;
            if ((categoriesByServer.get(serverId) || []).length >= 10) return serverError("other", "Limite de 10 categorias atingido.");
            await pool.query(
                `INSERT INTO categories (server_id, name, position)
                 VALUES ($1, $2, COALESCE((SELECT MAX(position) FROM categories WHERE server_id = $1), 0) + 1)`,
                [serverId, name]
            );
            await loadChannels();
            broadcastServer(serverId);
        } catch (error) {
            console.error("Erro ao criar categoria:", error);
        }
    });

    socket.on("category-rename", async data => {
        try {
            const categoryId = parseInt(data && data.categoryId, 10);
            const serverId = categoryIndex.get(categoryId);
            if (!(await requireManager(socket, serverId))) return serverError("other", "Você não tem permissão para renomear categorias.");
            const name = String((data && data.name) || "").replace(/[\u0000-\u001f\u007f]/g, "").trim().slice(0, 32);
            if (!name) return;
            await pool.query("UPDATE categories SET name = $1 WHERE id = $2 AND server_id = $3", [name, categoryId, serverId]);
            await loadChannels();
            broadcastServer(serverId);
        } catch (error) {
            console.error("Erro ao renomear categoria:", error);
        }
    });

    socket.on("category-delete", async data => {
        try {
            const categoryId = parseInt(data && data.categoryId, 10);
            const serverId = categoryIndex.get(categoryId);
            if (!(await requireManager(socket, serverId))) return serverError("other", "Você não tem permissão para excluir categorias.");
            await pool.query("DELETE FROM categories WHERE id = $1 AND server_id = $2", [categoryId, serverId]);
            await loadChannels();
            broadcastServer(serverId);
        } catch (error) {
            console.error("Erro ao excluir categoria:", error);
        }
    });

    socket.on("channel-move", async data => {
        try {
            const channelId = parseInt(data && data.channelId, 10);
            const serverId = channelIndex.get(channelId);
            if (!(await requireManager(socket, serverId))) return serverError("other", "Você não tem permissão para organizar canais.");
            let categoryId = data && data.categoryId ? parseInt(data.categoryId, 10) : null;
            if (categoryId && categoryIndex.get(categoryId) !== serverId) return;
            await pool.query("UPDATE channels SET category_id = $1 WHERE id = $2 AND server_id = $3", [categoryId || null, channelId, serverId]);
            await loadChannels();
            broadcastServer(serverId);
        } catch (error) {
            console.error("Erro ao mover canal:", error);
        }
    });

    // ----- Nome e foto do grupo -----
    socket.on("server-rename", async data => {
        try {
            const serverId = parseInt(data && data.serverId, 10);
            if (!serverId || serverId === mainServerId) return;
            if ((await actorRole(serverId)) !== "owner") return serverError("other", "Só o dono pode renomear o grupo.");
            const name = cleanServerName(data && data.name);
            if (!name) return serverError("other", "O nome do grupo precisa ter de 2 a 32 caracteres.");
            try {
                await pool.query("UPDATE servers SET name = $1 WHERE id = $2", [name, serverId]);
            } catch (error) {
                if (error && error.code === "23505") return serverError("other", "Já existe um grupo com esse nome.");
                throw error;
            }
            io.to("srv:" + serverId).emit("server-info", { serverId, name });
        } catch (error) {
            console.error("Erro ao renomear grupo:", error);
        }
    });

    socket.on("server-icon", async data => {
        try {
            const serverId = parseInt(data && data.serverId, 10);
            if (!(await requireManager(socket, serverId))) return serverError("other", "Você não tem permissão para trocar a foto do grupo.");
            const icon = data && data.icon ? data.icon : null;
            if (icon !== null && !validImage(icon)) return serverError("other", "Imagem inválida ou grande demais.");
            await pool.query("UPDATE servers SET icon = $1 WHERE id = $2", [icon, serverId]);
            io.to("srv:" + serverId).emit("server-info", { serverId, icon });
        } catch (error) {
            console.error("Erro ao trocar a foto do grupo:", error);
        }
    });

    // ----- Foto de perfil -----
    socket.on("avatar-set", async data => {
        try {
            if (!socket.data.user) return;
            const avatar = data && data.avatar ? data.avatar : null;
            if (avatar !== null && !validImage(avatar)) return serverError("other", "Imagem inválida ou grande demais.");
            await pool.query("UPDATE users SET avatar = $1 WHERE id = $2", [avatar, socket.data.user.id]);
            const sockets = userSockets.get(socket.data.user.id) || new Set([socket]);
            for (const s of sockets) {
                s.data.user.avatar = avatar;
                const room = s.data.currentRoom ? rooms.get(s.data.currentRoom) : null;
                const me = room && room.participants.get(s.id);
                if (me) {
                    me.avatar = avatar;
                    updateRoomState(s.data.currentRoom);
                }
                s.emit("avatar-saved", { avatar });
            }
        } catch (error) {
            console.error("Erro ao salvar a foto de perfil:", error);
        }
    });

    // ----- Membros e cargos -----
    async function sendMembers(serverId) {
        const rows = (await pool.query(
            `SELECT u.id, u.username, m.role
             FROM server_members m JOIN users u ON u.id = m.user_id
             WHERE m.server_id = $1 ORDER BY m.role, LOWER(u.username) LIMIT 300`,
            [serverId]
        )).rows;
        const members = rows.map(r => ({ userId: r.id, name: r.username, role: r.role, online: userSockets.has(r.id) }))
            .sort((a, b) => (ROLE_RANK[b.role] - ROLE_RANK[a.role]) || a.name.localeCompare(b.name));
        socket.emit("members-result", { serverId, members });
    }

    socket.on("members-list", async data => {
        try {
            const serverId = parseInt(data && data.serverId, 10);
            if (!(await actorRole(serverId))) return;
            await sendMembers(serverId);
        } catch (error) {
            console.error("Erro ao listar membros:", error);
        }
    });

    socket.on("member-role", async data => {
        try {
            const serverId = parseInt(data && data.serverId, 10);
            const targetId = parseInt(data && data.userId, 10);
            const role = data && data.role;
            if ((await actorRole(serverId)) !== "owner") return serverError("other", "Só o dono pode mudar cargos.");
            if (!["admin", "moderator", "member"].includes(role)) return;
            const targetRole = await getRole(targetId, serverId);
            if (!targetRole || targetRole === "owner") return;
            await pool.query("UPDATE server_members SET role = $1 WHERE server_id = $2 AND user_id = $3", [role, serverId, targetId]);
            for (const s of userSockets.get(targetId) || []) {
                if (s.data.roles) s.data.roles.set(serverId, role);
                s.emit("server-role", { serverId, role });
            }
            await sendMembers(serverId);
        } catch (error) {
            console.error("Erro ao mudar cargo:", error);
        }
    });

    socket.on("member-kick", async data => {
        try {
            const serverId = parseInt(data && data.serverId, 10);
            const targetId = parseInt(data && data.userId, 10);
            if (!serverId || serverId === mainServerId) return;
            const mine = await actorRole(serverId);
            if (!mine || ROLE_RANK[mine] < ROLE_RANK.moderator) return serverError("other", "Você não tem permissão para remover membros.");
            const targetRole = await getRole(targetId, serverId);
            if (!targetRole || targetId === socket.data.user.id) return;
            if (ROLE_RANK[targetRole] >= ROLE_RANK[mine]) return serverError("other", "Você não pode remover alguém com cargo igual ou maior que o seu.");
            await pool.query("DELETE FROM server_members WHERE server_id = $1 AND user_id = $2", [serverId, targetId]);
            for (const s of Array.from(userSockets.get(targetId) || [])) {
                const code = s.data.currentRoom;
                const room = code ? rooms.get(code) : null;
                if (room && room.serverId === serverId && s.data.leaveRoom) s.data.leaveRoom();
                if (s.data.roles) s.data.roles.delete(serverId);
                s.leave("srv:" + serverId);
                s.emit("server-removed", { serverId, reason: "kicked" });
            }
            await sendMembers(serverId);
        } catch (error) {
            console.error("Erro ao remover membro:", error);
        }
    });

    socket.on("leave-room", leaveCurrentRoom);
    socket.on("disconnect", () => {
        unregisterUserSocket();
        leaveCurrentRoom();
    });
});

function updateRoomState(roomCode) {
    const room = rooms.get(roomCode);
    if (!room) return;
    const participantsList = Array.from(room.participants.values());
    io.to(roomCode).emit("room-state", { participants: participantsList });
    if (room.channelId) broadcastServer(room.serverId);
}

const PORT = process.env.PORT || 3000;
initDb()
    .then(() => {
        console.log("Banco de dados conectado.");
        server.listen(PORT, () => {
            console.log("Servidor WEISX rodando na porta " + PORT);
        });
    })
    .catch(error => {
        console.error("Não foi possível iniciar o banco de dados:", error);
        process.exit(1);
    });
