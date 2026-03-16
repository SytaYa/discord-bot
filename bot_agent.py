#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
  BOT AGENT v5.23 — hébergé sur Render
  Variables : DISCORD_TOKEN  BOT_REMOTE_SECRET  BOT_REMOTE_ENABLED  PORT

  v5.0 : architecture initiale
  v5.2 : /menu slash, PostgreSQL, musique (yt_dlp + FFmpeg + Spotify)
  v5.3 : fix pipeline musique (timeout autocomplete, blocage silencieux,
         was_empty, global menu_locked, logs debug complets)
  v5.4 : fix définitif musique — YTDL_STREAM format corrigé, User-Agent FFmpeg,
         _auto_disconnect non-bloquant, after callback guard loop, logs CDN URL
  v5.5 : fix lecture audio — player_client ios, diagnostic boot FFmpeg+PyNaCl
  v5.6 : FFMPEG_OPTS simplifie (options minimales), capture stderr FFmpeg dans _after,
         test FFmpeg reel au boot (subprocess -version), logs etat vc avant/apres play()
  v5.7 : connexion vocale robuste (cas vc perime), music_play dispatch was_empty+join check,
         timeout stream 30s, console musique integree au menu principal
  v5.8 : asyncio.sleep remplace time.sleep dans _play_next, chargement Opus explicite
         au boot et dans on_ready, _now_playing_embed securise (title=None safe),
         log opus.is_loaded() avant vc.play(), diagnostic Opus dans after()
  v5.9 : (révoqué — mode PIPE causait boucle infinie : FFmpeg EOF avant que yt-dlp écrive)
  v5.10: CDN direct propre, guard anti-boucle _fail_count max 3,
         yt-dlp non épinglé (toujours à jour), YTDL_STREAM retries=5
  v5.11: logger yt_dlp custom (erreurs visibles Render), retry vanilla,
         android player_client, music_locked+message console
  v5.12: changelog manuel BDD (load/save), notification utilise changelog,
         dispatch update_check/update_publish
  v5.13: FIX AUDIO — reconnect_streamed remis (cause du silence), quiet=True yt_dlp
  v5.14: cwd=BOT_DIR portable (fix /app), priorité changelog stricte
  v5.15: /playtest diagnostic, timing after(), CMD FFmpeg loggée, PID FFmpeg
  v5.16: cookies YouTube via YT_COOKIES_B64 (fix bot check Render/AWS)
  v5.17: REFONTE MUSIQUE — Spotify (UX) + Deezer (audio), suppression yt_dlp/YouTube
  v5.18: Commandes musique retirées (/play /stop /skip /pause /resume /queue /volume)
  v5.19: /menu refait — navigation séparée Staff/Tickets, embeds modernes
  v5.20: fix maintenance /menu, commande /anon, toggle anon console+menu
  v5.21: panel admin complet, suivi activité membres, /activite, rôles exemptés
  v5.22: pagination universelle — membres/bots/activité/rôles exemptés (boutons ◀ ▶)
  v5.23: rapport activité complet (tous les rôles, bouton actualiser temps réel),
         persistance activité en BDD (survie aux redémarrages),
         scan automatique des vocaux au boot, toggle exempté corrigé
"""
import discord, asyncio, aiohttp, json, os, sys, hmac, hashlib, subprocess
import asyncpg
# yt_dlp supprimé — le système musique utilise maintenant Spotify + Deezer
YT_DLP_OK = False   # conservé pour compatibilité dispatch console existant
try:
    import spotipy
    from spotipy.oauth2 import SpotifyClientCredentials
    SPOTIPY_OK = True
except ImportError:
    SPOTIPY_OK = False
from discord import app_commands
from datetime import datetime, timezone
from collections import defaultdict
from aiohttp import web
from dotenv import load_dotenv

load_dotenv()

def log(tag: str, *args, **_):
    """Log horodate avec flush immediat — visible dans Render sans buffer.
    Accepte flush=True et autres kwargs pour compatibilité — ignorés (print utilise déjà flush=True).
    """
    from datetime import datetime
    ts  = datetime.now().strftime("%H:%M:%S")
    msg = " ".join(str(a) for a in args)
    print(f"[{ts}] [{tag}] {msg}", flush=True)

TOKEN          = os.getenv("DISCORD_TOKEN", "")
REMOTE_SECRET  = os.getenv("BOT_REMOTE_SECRET", "changeme")
REMOTE_ENABLED = os.getenv("BOT_REMOTE_ENABLED", "true").lower() == "true"
PORT           = int(os.getenv("PORT", 8080))
DATA_FILE      = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot_data.json")
BOT_DIR        = os.path.dirname(os.path.abspath(__file__))  # répertoire du bot (portable)
DATABASE_URL   = os.getenv("DATABASE_URL", "")   # URL PostgreSQL Supabase
_db_pool       = None   # pool de connexions asyncpg (initialisé au démarrage)

BOT_VERSION    = "5.23"  # version affichée dans le message de mise à jour

# ── Spotify credentials (optionnel) ──────────────────────────
SPOTIFY_CLIENT_ID     = os.getenv("SPOTIFY_CLIENT_ID", "")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET", "")

# Cookies YouTube supprimés (système musique migré vers Spotify + Deezer)
YT_COOKIES_OK = False  # conservé pour compatibilité

# ── État musique par serveur ──────────────────────────────────
# music_state[guild_id] = {
#   "queue":      [(title, url, duration, requester_name, thumbnail)],
#   "current":    (title, url, duration, requester_name, thumbnail) | None,
#   "volume":     0.5,          # 0.0 – 1.0
#   "np_channel": channel_id,   # salon où envoyer le "now playing"
# }
music_state = {}  # guild_id → dict

# ── État global ────────────────────────────────────────────────────
maintenance_mode = False          # GLOBAL — touche tous les serveurs
# menu_locked est maintenant LOCAL dans tconf(guild_id)["menu_locked"]
activity_tracker = defaultdict(dict)  # ancien tracker (compat)
activity_data: dict = {}  # nouveau système activité détaillé
ticket_config    = {}
ticket_sessions  = {}
pending_channels = {}
# Message de menu actif par serveur → (channel_id, message_id)
active_menu_msgs = {}

# ════════════════════════════════════════════════════════════════
#  PERSISTANCE POSTGRESQL (Supabase)
#  Variable d'env requise : DATABASE_URL
#  Format : postgresql://user:password@host:5432/postgres
# ════════════════════════════════════════════════════════════════

async def init_db():
    """Initialise le pool PostgreSQL. Appele au demarrage dans load_data()."""
    global _db_pool

    log("DB", ">>> Entree dans init_db()")

    # Verification DATABASE_URL
    if not DATABASE_URL:
        log("DB", "DATABASE_URL est VIDE ou non definie dans les variables d'env")
        log("DB", "-> Mode memoire uniquement (donnees perdues au redemarrage)")
        return

    safe_url = DATABASE_URL.split("@")[-1] if "@" in DATABASE_URL else DATABASE_URL
    log("DB", f"DATABASE_URL detectee -> hote : {safe_url}")
    log("DB", "Tentative de connexion PostgreSQL (SSL=require)...")

    try:
        _db_pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=1,
            max_size=5,
            ssl="require",
            command_timeout=30,
            server_settings={"application_name": "discord_bot"}
        )
        log("DB", "Pool de connexions cree OK")
    except Exception as e:
        log("DB", f"ECHEC connexion PostgreSQL : {e}")
        log("DB", "-> Fallback JSON active")
        _load_from_json()
        return

    # Creation des tables — requetes separees pour isoler les erreurs
    log("DB", "Creation / verification des tables...")
    try:
        async with _db_pool.acquire() as conn:
            log("DB", "  -> CREATE TABLE kv")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS kv (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)
            log("DB", "  -> CREATE TABLE guild_config")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS guild_config (
                    guild_id         BIGINT  PRIMARY KEY,
                    category_id      BIGINT,
                    staff_channel_id BIGINT,
                    staff_role_id    BIGINT,
                    counter          INTEGER DEFAULT 0,
                    menu_locked      BOOLEAN DEFAULT FALSE,
                    types_json       TEXT    DEFAULT '{}'
                )
            """)
            log("DB", "  -> CREATE TABLE ticket_session")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS ticket_session (
                    channel_id  BIGINT PRIMARY KEY,
                    guild_id    BIGINT NOT NULL,
                    data_json   TEXT   NOT NULL
                )
            """)
        log("DB", "Tables OK")
    except Exception as e:
        log("DB", f"ECHEC creation tables : {e}")
        await _db_pool.close()
        _db_pool = None
        log("DB", "-> Fallback JSON active")
        _load_from_json()
        return

    # Test de lecture pour confirmer que la DB repond vraiment
    try:
        async with _db_pool.acquire() as conn:
            n_cfg = await conn.fetchval("SELECT COUNT(*) FROM guild_config")
            n_tkt = await conn.fetchval("SELECT COUNT(*) FROM ticket_session")
        log("DB", f"Connexion confirmee : {n_cfg} config(s), {n_tkt} session(s) en base")
    except Exception as e:
        log("DB", f"ECHEC test lecture : {e}")

    await _maybe_migrate_json()


async def _maybe_migrate_json():
    """Migration unique bot_data.json -> PostgreSQL si la DB est vide."""
    if not _db_pool:
        return
    try:
        async with _db_pool.acquire() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM guild_config")
        if count == 0 and os.path.exists(DATA_FILE):
            log("DB", "DB vide + bot_data.json trouve -> migration en cours...")
            _load_from_json()
            if ticket_config or ticket_sessions:
                await save_data()
                log("DB", "Migration bot_data.json -> PostgreSQL terminee")
    except Exception as e:
        log("DB", f"Erreur migration : {e}")


async def save_data():
    """Sauvegarde configs et sessions en base (fallback JSON si DB indispo)."""
    if not _db_pool:
        _save_json_fallback()
        return
    try:
        async with _db_pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "INSERT INTO kv VALUES($1,$2) "
                    "ON CONFLICT(key) DO UPDATE SET value=$2",
                    "maintenance_mode", "1" if maintenance_mode else "0")
                for gid, cfg in ticket_config.items():
                    await conn.execute("""
                        INSERT INTO guild_config
                            (guild_id, category_id, staff_channel_id, staff_role_id,
                             counter, menu_locked, types_json)
                        VALUES ($1,$2,$3,$4,$5,$6,$7)
                        ON CONFLICT(guild_id) DO UPDATE SET
                            category_id      = EXCLUDED.category_id,
                            staff_channel_id = EXCLUDED.staff_channel_id,
                            staff_role_id    = EXCLUDED.staff_role_id,
                            counter          = EXCLUDED.counter,
                            menu_locked      = EXCLUDED.menu_locked,
                            types_json       = EXCLUDED.types_json
                    """,
                    gid,
                    cfg.get("category_id"),
                    cfg.get("staff_channel_id"),
                    cfg.get("staff_role_id"),
                    cfg.get("counter", 0),
                    bool(cfg.get("menu_locked")),
                    json.dumps({
                        "types":                   cfg.get("types", {}),
                        "music_enabled":           cfg.get("music_enabled", True),
                        "music_role_id":           cfg.get("music_role_id"),
                        "music_volume":            cfg.get("music_volume", 50),
                        "anon_enabled":            cfg.get("anon_enabled", True),
                        "activity_exempt_role_ids": cfg.get("activity_exempt_role_ids", []),
                        "music_locked":            cfg.get("music_locked", False),
                        "music_lock_msg":          cfg.get("music_lock_msg", ""),
                    }, ensure_ascii=False))
                # Sauvegarder activity_data par serveur dans kv
                for gid_a, gdata in activity_data.items():
                    serializable = {}
                    for mid, d in gdata.items():
                        serializable[str(mid)] = {
                            "msg_count":     d.get("msg_count", 0),
                            "msg_last":      d["msg_last"].isoformat() if d.get("msg_last") else None,
                            "voice_seconds": d.get("voice_seconds", 0),
                            "last_seen":     d["last_seen"].isoformat() if d.get("last_seen") else None,
                            # voice_joined non persisté (recalculé au boot)
                        }
                    await conn.execute(
                        "INSERT INTO kv VALUES($1,$2) ON CONFLICT(key) DO UPDATE SET value=$2",
                        f"activity_{gid_a}", json.dumps(serializable))
                await conn.execute("DELETE FROM ticket_session")
                for ch_id, s in ticket_sessions.items():
                    await conn.execute(
                        "INSERT INTO ticket_session VALUES ($1,$2,$3)",
                        ch_id, s["guild_id"],
                        json.dumps(s, ensure_ascii=False))
    except Exception as e:
        log("DB", f"Erreur save_data : {e} -> fallback JSON")
        _save_json_fallback()


async def load_data():
    """Charge toutes les donnees depuis PostgreSQL au demarrage."""
    global ticket_config, ticket_sessions, maintenance_mode

    log("DB", "Chargement des donnees...")
    await init_db()

    if not _db_pool:
        log("DB", "Pool indisponible — donnees depuis JSON (ou memoire vide)")
        return

    try:
        async with _db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT value FROM kv WHERE key='maintenance_mode'")
            if row:
                maintenance_mode = row["value"] == "1"
            rows = await conn.fetch("SELECT * FROM guild_config")
            for row in rows:
                raw_json = json.loads(row["types_json"] or "{}")
                # Compatibilité : ancien format = dict de types directement
                if "types" in raw_json:
                    types_data         = raw_json.get("types", {})
                    music_enabled_data = raw_json.get("music_enabled", True)
                    music_role_data    = raw_json.get("music_role_id")
                    music_volume_data  = raw_json.get("music_volume", 50)
                else:
                    types_data         = raw_json   # ancien format
                    music_enabled_data = True
                    music_role_data    = None
                    music_volume_data  = 50
                ticket_config[row["guild_id"]] = {
                    "category_id":             row["category_id"],
                    "staff_channel_id":        row["staff_channel_id"],
                    "staff_role_id":           row["staff_role_id"],
                    "counter":                 row["counter"],
                    "menu_locked":             bool(row["menu_locked"]),
                    "types":                   types_data,
                    "music_enabled":           music_enabled_data,
                    "music_role_id":           music_role_data,
                    "music_volume":            music_volume_data,
                    "anon_enabled":            raw_json.get("anon_enabled", True),
                    "activity_exempt_role_ids": raw_json.get("activity_exempt_role_ids", []),
                    "music_locked":            raw_json.get("music_locked", False),
                    "music_lock_msg":          raw_json.get("music_lock_msg", ""),
                }
            rows = await conn.fetch("SELECT * FROM ticket_session")
            for row in rows:
                ticket_sessions[row["channel_id"]] = json.loads(row["data_json"])
        # Charger activity_data
        act_rows = await conn.fetch("SELECT key, value FROM kv WHERE key LIKE 'activity_%'")
        for arow in act_rows:
            gid_str = arow["key"].replace("activity_", "")
            try:
                gid_int = int(gid_str)
                gdata   = json.loads(arow["value"])
                activity_data[gid_int] = {}
                for mid_str, d in gdata.items():
                    activity_data[gid_int][int(mid_str)] = {
                        "msg_count":     d.get("msg_count", 0),
                        "msg_last":      datetime.fromisoformat(d["msg_last"]).replace(tzinfo=timezone.utc) if d.get("msg_last") else None,
                        "voice_seconds": d.get("voice_seconds", 0),
                        "voice_joined":  None,  # recalculé ci-dessous
                        "last_seen":     datetime.fromisoformat(d["last_seen"]).replace(tzinfo=timezone.utc) if d.get("last_seen") else None,
                    }
            except Exception as _ea:
                log("DB", f"Erreur chargement activity {gid_str}: {_ea}")
        log("DB", f"Charge : {len(ticket_config)} guild(s), {len(ticket_sessions)} ticket(s)"
            f" | maintenance={'ON' if maintenance_mode else 'OFF'}")
    except Exception as e:
        log("DB", f"Erreur load_data : {e} -> fallback JSON")
        _load_from_json()


def _save_json_fallback():
    """Sauvegarde JSON d'urgence si PostgreSQL est inaccessible."""
    try:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "ticket_config":   {str(k): v for k, v in ticket_config.items()},
                "ticket_sessions": {str(k): v for k, v in ticket_sessions.items()},
                "maintenance_mode": maintenance_mode
            }, f, ensure_ascii=False, indent=2)
        log("DB", "Fallback JSON sauvegarde")
    except Exception as e:
        log("DB", f"Erreur JSON fallback : {e}")


def _load_from_json():
    """Charge depuis bot_data.json (fallback ou migration)."""
    global ticket_config, ticket_sessions, maintenance_mode
    if not os.path.exists(DATA_FILE):
        log("DB", "bot_data.json introuvable — demarrage avec donnees vides")
        return
    try:
        with open(DATA_FILE, encoding="utf-8") as f:
            d = json.load(f)
        for k, v in d.get("ticket_config", {}).items():
            ticket_config[int(k)] = v
        for k, v in d.get("ticket_sessions", {}).items():
            ticket_sessions[int(k)] = v
        maintenance_mode = d.get("maintenance_mode", False)
        log("DB", f"JSON charge : {len(ticket_config)} guild(s), {len(ticket_sessions)} ticket(s)")
    except Exception as e:
        log("DB", f"Erreur lecture JSON : {e}")



def tconf(gid):
    """Retourne la config du serveur gid, en l'initialisant si absente."""
    if gid not in ticket_config:
        ticket_config[gid] = {
            "category_id":      None,
            "staff_channel_id": None,
            "staff_role_id":    None,
            "counter":          0,
            "types":            {},
            "menu_locked":      False,
            # champs musique
            "music_enabled":    True,   # True = tout le monde, False = rôle requis
            "music_role_id":    None,   # rôle autorisé si music_enabled=False
            "music_volume":     50,     # volume par défaut (0–100)
            "music_locked":     False,  # True = musique verrouillée globalement
            "music_lock_msg":   "",     # message affiché quand la musique est verrouillée
            "anon_enabled":     True,   # True = messages anonymes autorisés
        }
    if "menu_locked" not in ticket_config[gid]:
        ticket_config[gid]["menu_locked"] = False
    # Migrations des champs musique manquants (serveurs existants)
    cfg = ticket_config[gid]
    if "music_enabled"  not in cfg: cfg["music_enabled"]  = True
    if "music_role_id"  not in cfg: cfg["music_role_id"]  = None
    if "music_volume"   not in cfg: cfg["music_volume"]   = 50
    if "music_locked"   not in cfg: cfg["music_locked"]   = False
    if "music_lock_msg" not in cfg: cfg["music_lock_msg"] = ""
    if "anon_enabled"          not in cfg: cfg["anon_enabled"]          = True
    if "activity_exempt_role_ids" not in cfg: cfg["activity_exempt_role_ids"] = []
    return ticket_config[gid]

def is_configured(gid):
    """Retourne (True, None) si config OK, sinon (False, message erreur)."""
    cfg = tconf(gid)
    missing = []
    if not cfg.get("category_id"):      missing.append("catégorie des tickets")
    if not cfg.get("staff_channel_id"): missing.append("salon staff")
    if not cfg.get("staff_role_id"):    missing.append("rôle staff")
    if missing:
        return False, "Config incomplète — manque : " + ", ".join(missing) + ". Utilise `/menu` → Configuration."
    return True, None

# ── Palette de couleurs (utilisée partout) ────────────────────────
C_ORANGE  = 0xF0A500
C_AMBER   = 0xE67E22
C_GREEN   = 0x57F287
C_RED     = 0xED4245
C_BLUE    = 0x3498DB
C_PURPLE  = 0x9B59B6
C_TEAL    = 0x1ABC9C
C_DARK    = 0x2B2D31
C_BLURPLE = 0x5865F2
C_GREY    = 0x36393F

# ── Client Discord ─────────────────────────────────────────────────
intents = discord.Intents.all()
client  = discord.Client(intents=intents)
tree    = app_commands.CommandTree(client)

# ── Helpers ────────────────────────────────────────────────────────
def find_member(arg, guild):
    humans = [m for m in guild.members if not m.bot]
    try:
        idx = int(arg)
        if 1 <= idx <= len(humans): return humans[idx - 1]
        m = guild.get_member(idx)
        if m: return m
    except ValueError:
        pass
    hits = [m for m in humans if arg.lower() in m.display_name.lower() or arg.lower() in m.name.lower()]
    return hits[0] if len(hits) == 1 else None

def find_channel(arg, guild):
    chs = [c for c in guild.channels if isinstance(c, discord.TextChannel)]
    try:
        idx = int(arg.lstrip("#"))
        if 1 <= idx <= len(chs): return chs[idx - 1]
        c = guild.get_channel(idx)
        if c: return c
    except ValueError:
        pass
    for c in chs:
        if arg.lstrip("#").lower() in c.name.lower(): return c
    return None

def is_staff(member, guild_id):
    if member.guild_permissions.administrator: return True
    cfg = ticket_config.get(guild_id, {})
    if cfg.get("staff_role_id"):
        r = member.guild.get_role(cfg["staff_role_id"])
        if r and r in member.roles: return True
    return False

# ── Tickets ────────────────────────────────────────────────────────
async def open_ticket(guild, member, type_key):
    if maintenance_mode:
        return None, "Bot en maintenance — les tickets sont temporairement désactivés."
    ok_cfg, msg_cfg = is_configured(guild.id)
    if not ok_cfg: return None, msg_cfg
    cfg = tconf(guild.id)
    if type_key not in cfg["types"]:
        return None, "Type inconnu : `" + type_key + "`. Disponibles : " + ", ".join("`" + k + "`" for k in cfg["types"])
    cat = guild.get_channel(cfg["category_id"])
    if not cat: return None, "Catégorie introuvable (id=" + str(cfg["category_id"]) + ")"
    tt = cfg["types"][type_key]
    cfg["counter"] += 1
    n = cfg["counter"]
    ow = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        member: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True),
        guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_channels=True, read_message_history=True),
    }
    if cfg.get("staff_role_id"):
        sr = guild.get_role(cfg["staff_role_id"])
        if sr: ow[sr] = discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_messages=True, read_message_history=True)
    try:
        ch = await guild.create_text_channel(
            "ticket-" + str(n).zfill(4) + "-" + member.display_name[:12].lower().replace(" ", "-"),
            category=cat, overwrites=ow,
            topic="Ticket #" + str(n) + " | " + type_key + " | " + member.display_name)
    except discord.Forbidden:
        cfg["counter"] -= 1
        return None, "Permission refusée"
    s = {"guild_id": guild.id, "user_id": member.id, "type": type_key, "status": "open", "answers": {},
         "opened_at": datetime.now().strftime("%d/%m/%Y %H:%M"), "number": n, "channel_id": ch.id}
    ticket_sessions[ch.id] = s
    await save_data()
    await ch.send("👋 " + member.mention + " — Ticket **#" + str(n).zfill(4) + "** " + tt["emoji"] + " " + tt["label"] + "\n" + "━" * 40)
    await _notify_embed(guild, cfg, s, tt)
    return ch, None

async def run_questions(guild, cfg, channel, member, type_key, existing_session=None):
    """
    Lance les questions dans le salon.
    existing_session : session déjà créée par open_ticket → réutilisée sans incrémenter le compteur.
    """
    if maintenance_mode:
        await channel.send("🔧 Bot en maintenance — les questions sont temporairement suspendues.")
        return None, "maintenance"
    tt = cfg["types"][type_key]
    qs = tt.get("questions", [])
    if existing_session:
        s = existing_session
        s["status"] = "pending"
        n = s["number"]
    else:
        cfg["counter"] += 1
        n = cfg["counter"]
        s = {"guild_id": guild.id, "user_id": member.id, "type": type_key, "status": "pending", "answers": {},
             "opened_at": datetime.now().strftime("%d/%m/%Y %H:%M"), "number": n, "channel_id": channel.id}
        ticket_sessions[channel.id] = s
    await save_data()
    pending_channels.pop(channel.id, None)
    try:
        await channel.set_permissions(guild.me, view_channel=True, send_messages=True,
                                      read_message_history=True, reason="Bot tickets")
    except Exception as e:
        return None, str(e)
    await channel.send("👋 " + member.mention + " — Questions en cours.\n" + "━" * 40)
    for q in qs:
        await channel.send("❓ **" + q + "**")
        r = await client.wait_for("message",
            check=lambda m, mb=member, ch=channel: m.author == mb and m.channel == ch)
        s["answers"][q] = r.content
    lines = ["━" * 40, "🎫 **#" + str(n).zfill(4) + "** " + tt["emoji"] + " " + tt["label"], "👤 " + member.mention]
    for q, a in s["answers"].items():
        lines += ["❓ **" + q + "**", "💬 " + a]
    lines.append("━" * 40)
    await channel.send("\n".join(lines))
    await _notify_reactions(guild, cfg, s, tt)
    await save_data()
    return s, None

async def _notify_embed(guild, cfg, s, tt):
    if not cfg.get("staff_channel_id"): return
    sc = guild.get_channel(cfg["staff_channel_id"])
    if not sc: return
    rm = "<@&" + str(cfg["staff_role_id"]) + "> " if cfg.get("staff_role_id") else ""
    e = discord.Embed(color=0xF0A500, timestamp=datetime.now(timezone.utc))
    e.set_author(name="🎫 Nouveau ticket #" + str(s["number"]).zfill(4))
    e.add_field(name="Type",      value=tt["emoji"] + " " + tt["label"], inline=True)
    e.add_field(name="Demandeur", value="<@" + str(s["user_id"]) + ">",  inline=True)
    e.add_field(name="Salon",     value="<#" + str(s["channel_id"]) + ">", inline=True)
    msg = await sc.send(content=rm, embed=e, view=TicketActionView(guild, s))
    s["staff_msg_id"] = msg.id
    s["staff_chan_id"] = sc.id
    await save_data()

async def _notify_reactions(guild, cfg, s, tt):
    if not cfg.get("staff_channel_id"): return
    sc = guild.get_channel(cfg["staff_channel_id"])
    if not sc: return
    rm  = "<@&" + str(cfg["staff_role_id"]) + "> " if cfg.get("staff_role_id") else ""
    ans = "\n".join("❓ " + q + "\n💬 " + a for q, a in s["answers"].items()) or "*(aucune)*"
    e = discord.Embed(color=0xE67E22, timestamp=datetime.now(timezone.utc))
    e.set_author(name="🎫 Ticket #" + str(s["number"]).zfill(4) + " — Réponses")
    e.add_field(name="Type",      value=tt["emoji"] + " " + tt["label"], inline=True)
    e.add_field(name="Demandeur", value="<@" + str(s["user_id"]) + ">",  inline=True)
    e.add_field(name="Salon",     value="<#" + str(s["channel_id"]) + ">", inline=True)
    e.add_field(name="Réponses",  value=ans[:1024], inline=False)
    msg = await sc.send(content=rm, embed=e, view=TicketActionView(guild, s))
    await msg.add_reaction("✅")
    await msg.add_reaction("❌")
    s["staff_msg_id"] = msg.id
    s["staff_chan_id"] = sc.id
    await save_data()

async def _disable_ticket_buttons(guild, s):
    """
    Supprime les boutons du message staff après décision.
    view=None = boutons complètement retirés, embed reste visible.
    """
    msg_id  = s.get("staff_msg_id")
    chan_id  = s.get("staff_chan_id")
    if not msg_id or not chan_id: return
    ch = guild.get_channel(chan_id)
    if not ch: return
    try:
        msg = await ch.fetch_message(msg_id)
        await msg.edit(view=None)   # retire tous les boutons, garde l'embed
    except Exception as e:
        log("BUTTONS", str(e))

async def do_action(guild, s, action, raison="", actor=None):
    """
    Exécute une action sur un ticket.
    — La décision (embed complet) est postée UNIQUEMENT dans le salon staff.
    — Un court message informatif est envoyé dans le salon ticket pour le demandeur.
    """
    cfg = tconf(guild.id)
    ch  = guild.get_channel(s["channel_id"])
    who = actor.mention if actor else "le staff"
    sc  = guild.get_channel(cfg["staff_channel_id"]) if cfg.get("staff_channel_id") else None

    if action == "accepted":
        s["status"] = "accepted"; await save_data()
        # Désactiver les boutons du message staff (persistant)
        await _disable_ticket_buttons(guild, s)
        # Salon staff — embed complet
        if sc:
            e = discord.Embed(color=C_GREEN, timestamp=datetime.now(timezone.utc))
            e.set_author(name="✅  Ticket #" + str(s["number"]).zfill(4) + " — Accepté")
            e.add_field(name="Par",   value=who, inline=True)
            e.add_field(name="Salon", value="<#" + str(s["channel_id"]) + ">", inline=True)
            if raison: e.add_field(name="Raison", value=raison, inline=False)
            await sc.send(embed=e)
        # Salon ticket — message court pour informer le demandeur
        if ch:
            try:
                msg = "✅ Ton ticket a été **accepté**."
                if raison: msg += "\n> " + raison
                await ch.send(msg)
            except: pass

    elif action == "refused":
        s["status"] = "refused"; await save_data()
        # Désactiver les boutons du message staff (persistant)
        await _disable_ticket_buttons(guild, s)
        if sc:
            e = discord.Embed(color=C_RED, timestamp=datetime.now(timezone.utc))
            e.set_author(name="❌  Ticket #" + str(s["number"]).zfill(4) + " — Refusé")
            e.add_field(name="Par",   value=who, inline=True)
            e.add_field(name="Salon", value="<#" + str(s["channel_id"]) + ">", inline=True)
            if raison: e.add_field(name="Raison", value=raison, inline=False)
            await sc.send(embed=e)
        if ch:
            try:
                msg = "❌ Ton ticket a été **refusé**."
                if raison: msg += "\n> " + raison
                await ch.send(msg)
            except: pass

    elif action == "closed":
        s["status"] = "closed"; await save_data()
        # Désactiver les boutons du message staff (persistant)
        await _disable_ticket_buttons(guild, s)
        if sc:
            e = discord.Embed(color=C_GREY, timestamp=datetime.now(timezone.utc))
            e.set_author(name="🔒  Ticket #" + str(s["number"]).zfill(4) + " — Fermé")
            e.add_field(name="Par", value=who, inline=True)
            await sc.send(embed=e)
        if ch:
            try:
                await ch.send("🔒 Ce salon sera supprimé dans 5 secondes.")
                await asyncio.sleep(5)
                await ch.delete()
            except: pass
        ticket_sessions.pop(s["channel_id"], None)
        await save_data()

# ── Actions enrichies à l'acceptation ─────────────────────────────
async def do_accept_actions(guild, s, actions: dict):
    """
    actions = {
      "rename": "pseudo",   # renommer le demandeur
      "invite": True,       # créer lien invitation 1 usage
      "add_role": role_id,  # attribuer un rôle existant
      "create_role": "nom", # créer un rôle et l'attribuer
    }
    """
    member = guild.get_member(s["user_id"])
    ch     = guild.get_channel(s["channel_id"])
    errors = []
    if not member: return ["Membre introuvable (a-t-il quitté le serveur ?)"]

    if actions.get("rename"):
        try: await member.edit(nick=str(actions["rename"])[:32])
        except discord.Forbidden: errors.append("Rename : permission refusée")

    if actions.get("invite") and ch:
        try:
            invite = await ch.create_invite(max_uses=1, unique=True,
                reason="Acceptation ticket #" + str(s["number"]))
            await ch.send("🔗 **Lien d'invitation** (1 utilisation) : " + invite.url)
        except discord.Forbidden: errors.append("Invite : permission refusée")

    if actions.get("add_role"):
        role = guild.get_role(int(actions["add_role"]))
        if role:
            try: await member.add_roles(role, reason="Ticket accepté #" + str(s["number"]))
            except discord.Forbidden: errors.append("Add_role : permission refusée")
        else: errors.append("Add_role : rôle introuvable")

    if actions.get("create_role"):
        try:
            new_role = await guild.create_role(name=str(actions["create_role"]),
                reason="Ticket #" + str(s["number"]))
            await member.add_roles(new_role, reason="Ticket accepté #" + str(s["number"]))
            if ch: await ch.send("🎭 Rôle **" + new_role.name + "** créé et attribué.")
        except discord.Forbidden: errors.append("Create_role : permission refusée")

    return errors

# ── Gestion du menu actif (1 seul par serveur) ─────────────────────
async def _delete_active_menu(guild_id: int, channel=None):
    """
    Supprime l'ancien message de menu pour ce serveur.
    Stratégie : d'abord via active_menu_msgs (en mémoire),
    ensuite scan des 15 derniers messages du salon si channel fourni.
    """
    # 1. Suppression via ID mémorisé
    entry = active_menu_msgs.pop(guild_id, None)
    if entry:
        chan_id, msg_id = entry
        g = client.get_guild(guild_id)
        if g:
            ch = g.get_channel(chan_id)
            if ch:
                try:
                    msg = await ch.fetch_message(msg_id)
                    await msg.delete()
                    return   # supprimé proprement, pas besoin de scanner
                except: pass

    # 2. Scan fallback : supprimer les messages du bot avec le panel dans ce salon
    if channel:
        try:
            async for msg in channel.history(limit=15):
                if msg.author == channel.guild.me and msg.embeds:
                    emb = msg.embeds[0]
                    author_name = emb.author.name if emb.author else ""
                    if "Panel Staff" in author_name:
                        try: await msg.delete()
                        except: pass
                        break   # un seul menu à la fois
        except: pass

# ── Changelog manuel (stocké en BDD, clé "changelog") ─────────────────
# Structure JSON : {"version": "5.x", "description": "...", "updated_at": "..."}
# Prioritaire sur le message de commit Git pour le message Discord public.

_cached_changelog: dict | None = None   # chargé une fois au démarrage

async def load_changelog() -> dict | None:
    """Charge le changelog manuel depuis la BDD (clé kv 'changelog')."""
    global _cached_changelog
    if not _db_pool:
        return None
    try:
        async with _db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT value FROM kv WHERE key='changelog'")
            if row:
                _cached_changelog = json.loads(row["value"])
                log("BOT", f"Changelog chargé : v{_cached_changelog.get('version','?')} — {_cached_changelog.get('description','')[:60]}")
                return _cached_changelog
    except Exception as e:
        log("BOT", f"load_changelog erreur : {e}")
    return None

async def save_changelog(version: str, description: str) -> bool:
    """Sauvegarde le changelog manuel en BDD (clé kv 'changelog')."""
    global _cached_changelog
    data = {
        "version":     version,
        "description": description,
        "updated_at":  datetime.now(timezone.utc).isoformat(),
        "source":      "dev",
        "target":      "main",
    }
    _cached_changelog = data
    if not _db_pool:
        log("BOT", "save_changelog : pas de BDD disponible")
        return False
    try:
        async with _db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO kv VALUES($1,$2) ON CONFLICT(key) DO UPDATE SET value=$2",
                "changelog", json.dumps(data)
            )
        log("BOT", f"Changelog sauvegardé : v{version}")
        return True
    except Exception as e:
        log("BOT", f"save_changelog erreur : {e}")
        return False

# ── Événements Discord ─────────────────────────────────────────────
def get_last_commit_message() -> str:
    """Lit le dernier message de commit Git (utilisé comme fallback si pas de changelog manuel)."""
    try:
        result = subprocess.run(
            ["git", "log", "-1", "--pretty=%B"],
            capture_output=True, text=True, timeout=5
        )
        msg = result.stdout.strip()
        return msg if msg else "Mise à jour déployée."
    except Exception as e:
        log("BOT", f"get_last_commit_message erreur : {e}")
        return "Mise à jour déployée."


async def send_update_notifications():
    """Envoie un embed de mise à jour dans chaque salon staff configuré.

    Priorité stricte :
      1. Changelog manuel (saisi depuis la console distante) — version ET description
      2. Fallback uniquement si aucun changelog manuel valide n'existe :
         BOT_VERSION + dernier commit Git
    Le commit Git n'est JAMAIS utilisé si un changelog manuel existe.
    """
    cl = _cached_changelog
    # Vérifier que le changelog manuel est réellement complet (version ET description non vides)
    cl_version     = cl.get("version", "").strip()     if cl else ""
    cl_description = cl.get("description", "").strip() if cl else ""
    has_manual_cl  = bool(cl_version and cl_description)

    if has_manual_cl:
        version_str = cl_version
        description = cl_description
        log("BOT", f"Notification — changelog manuel v{version_str} : {description[:60]}")
    else:
        # Fallback : aucun changelog manuel valide → BOT_VERSION + commit Git
        version_str = BOT_VERSION
        description = get_last_commit_message()
        log("BOT", f"Notification — fallback commit : {description[:60]}")

    sent = 0
    for g in client.guilds:
        cfg = ticket_config.get(g.id)
        if not cfg or not cfg.get("staff_channel_id"):
            continue
        ch = g.get_channel(cfg["staff_channel_id"])
        if not ch:
            log("BOT", f"[{g.name}] salon staff introuvable (id={cfg['staff_channel_id']})")
            continue
        if not ch.permissions_for(g.me).send_messages:
            log("BOT", f"[{g.name}] permission refusée dans #{ch.name}")
            continue
        try:
            e = discord.Embed(
                title="✅  Bot mis à jour",
                description=description,
                color=C_GREEN,
                timestamp=datetime.now(timezone.utc)
            )
            e.add_field(name="🔖  Version", value=f"`{version_str}`", inline=True)
            e.add_field(name="🌐  Serveur", value=g.name, inline=True)
            e.set_footer(text=client.user.name)
            await ch.send(embed=e)
            sent += 1
            log("BOT", f"[{g.name}] notification envoyée dans #{ch.name}")
        except Exception as ex:
            log("BOT", f"[{g.name}] erreur envoi notification : {ex}")

    log("BOT", f"Notifications envoyées : {sent}/{len(client.guilds)} serveur(s)")



@tree.command(name="menu", description="Ouvrir le panel de gestion staff")
async def slash_menu(inter: discord.Interaction):
    try:
        if not inter.guild:
            await inter.response.send_message("Commande disponible sur un serveur uniquement.", ephemeral=True)
            return
        if maintenance_mode:
            await inter.response.send_message(
                embed=discord.Embed(
                    title="🔧  Bot en maintenance",
                    description="Le bot est temporairement indisponible. Réessaie dans quelques instants.",
                    color=0xE67E22),
                ephemeral=True)
            return
        if not is_staff(inter.user, inter.guild.id):
            await inter.response.send_message("🚫 Réservé au staff configuré.", ephemeral=True)
            return
        await send_staff_menu_inter(inter, inter.guild)
    except Exception as e:
        log("SLASH", f"/menu : {e}")
        try:
            await inter.response.send_message("❌ Erreur.", ephemeral=True)
        except Exception:
            pass


@tree.command(name="anon", description="Envoyer un message anonyme dans ce salon")
@app_commands.describe(message="Ton message (tu restes anonyme)")
async def slash_anon(inter: discord.Interaction, message: str):
    try:
        if not inter.guild:
            await inter.response.send_message("Commande disponible sur un serveur uniquement.", ephemeral=True)
            return
        if maintenance_mode:
            await inter.response.send_message(
                embed=discord.Embed(title="🔧  Bot en maintenance", color=0xE67E22),
                ephemeral=True); return
        cfg = tconf(inter.guild.id)
        if not cfg.get("anon_enabled", True):
            await inter.response.send_message(
                embed=discord.Embed(
                    title="🔇  Messages anonymes désactivés",
                    description="L'administration a bloqué les messages anonymes sur ce serveur.",
                    color=0xED4245),
                ephemeral=True)
            return
        if not message.strip():
            await inter.response.send_message("❌ Le message ne peut pas être vide.", ephemeral=True)
            return
        # Envoyer le message anonyme dans le salon courant
        e = discord.Embed(
            description=message.strip(),
            color=0x2B2D31,
            timestamp=datetime.now(timezone.utc)
        )
        e.set_author(name="💬  Message anonyme d'un membre du serveur")
        e.set_footer(text=inter.guild.name, icon_url=inter.guild.icon.url if inter.guild.icon else None)
        await inter.channel.send(embed=e)
        # Confirmer à l'expéditeur (éphémère — lui seul le voit)
        await inter.response.send_message(
            embed=discord.Embed(
                title="✅  Message envoyé",
                description="Ton message a été transmis anonymement.",
                color=0x57F287),
            ephemeral=True)
        log("ANON", f"[{inter.guild.name}] message anonyme envoyé dans #{inter.channel.name}", flush=True)
    except Exception as e:
        log("SLASH", f"/anon : {e}")
        try: await inter.response.send_message("❌ Erreur.", ephemeral=True)
        except Exception: pass


@tree.command(name="activite", description="Voir le rapport d'activité d'un membre (staff uniquement)")
@app_commands.describe(membre="Membre à analyser (laisser vide pour la liste complète)")
async def slash_activite(inter: discord.Interaction, membre: discord.Member = None):
    try:
        if not inter.guild:
            await inter.response.send_message("Commande disponible sur un serveur uniquement.", ephemeral=True); return
        if maintenance_mode:
            await inter.response.send_message(embed=discord.Embed(title="🔧  Maintenance", color=0xE67E22), ephemeral=True); return
        if not is_staff(inter.user, inter.guild.id):
            await inter.response.send_message("🚫  Réservé au staff.", ephemeral=True); return
        if membre:
            # Rapport individuel
            exempt = _is_exempt(membre, inter.guild.id)
            e = _member_activity_embed(membre, inter.guild.id)
            await inter.response.send_message(embed=e, ephemeral=True)
        else:
            # Liste complète
            e = _build_embed_activity(inter.guild, show_exempt=True)
            await inter.response.send_message(embed=e, view=ActivityView(inter.guild), ephemeral=True)
    except Exception as ex:
        log("SLASH", f"/activite : {ex}")
        try: await inter.response.send_message("❌ Erreur.", ephemeral=True)
        except Exception: pass


@client.event
async def on_ready():
    log("BOT", "Connecte en tant que", str(client.user))
    log("BOT", f"{len(client.guilds)} serveur(s) visibles")
    # ── Chargement et vérification Opus ──────────────────────────────────
    # discord.py a besoin d'Opus pour encoder le PCM en paquets vocaux.
    # Sans Opus, vc.play() s'exécute mais n'envoie aucun son — silencieux.
    try:
        if not discord.opus.is_loaded():
            # Essayer les noms courants selon la plateforme
            for _lib in ("libopus.so.0", "libopus.so", "opus", "libopus-0.dll"):
                try:
                    discord.opus.load_opus(_lib)
                    if discord.opus.is_loaded():
                        log("AUDIO", f"Opus chargé manuellement : {_lib} ✔")
                        break
                except Exception:
                    continue
        if discord.opus.is_loaded():
            log("AUDIO", "Opus : chargé ✔ (encodage vocal Discord OK)")
        else:
            log("AUDIO", "Opus : NON CHARGÉ ❌ — vc.play() sera silencieux !")
            log("AUDIO", "  -> Vérifier : RUN apt-get install -y libopus0 dans le Dockerfile")
            log("AUDIO", "  -> discord.py tente un chargement automatique au premier play()")
    except Exception as _e_opus:
        log("AUDIO", f"Opus : exception lors du chargement : {_e_opus}")
    await load_data()
    # Restaurer les views persistantes pour les tickets en attente
    # On passe message_id= pour que discord.py rattache la view
    # au bon message (sinon les boutons après redémarrage ne fonctionnent pas)
    restored = 0
    for s in ticket_sessions.values():
        if s.get("status") not in ("open", "pending"): continue
        g = client.get_guild(s["guild_id"])
        if not g: continue
        msg_id = s.get("staff_msg_id")
        if msg_id:
            client.add_view(TicketActionView(g, s), message_id=msg_id)
        else:
            client.add_view(TicketActionView(g, s))
        restored += 1
    if restored: log("VIEWS", str(restored) + " view(s) persistante(s) restauree(s)")
    # Synchronisation slash commands
    # On ne fait PAS de purge globale à chaque redémarrage : ça génère
    # "Intégration inconnue" sur Discord car la commande est supprimée puis
    # recréée avec un nouvel ID à chaque boot.
    # Stratégie : sync directe sans effacer — Discord ne recrée que si nécessaire.
    try:
        # S'assurer que /menu est bien dans le tree local avant le sync
        existing = {c.name for c in tree.get_commands()}
        for name, fn in {"menu": slash_menu, "anon": slash_anon, "activite": slash_activite}.items():
            if name not in existing:
                tree.add_command(fn)
                log("SLASH", f"{name} ajouté au tree local")
        synced = await tree.sync()
        log("SLASH", f"Commandes sync : {[c.name for c in synced]}")
    except discord.errors.HTTPException as e:
        log("SLASH", f"Erreur HTTP sync : {e.status} {e.text}")
    except Exception as e:
        log("SLASH", f"Erreur sync : {e}")
    total = 0
    for g in client.guilds:
        cfg = ticket_config.get(g.id)
        if not cfg or not cfg.get("category_id"): continue
        cat = g.get_channel(cfg["category_id"])
        if not cat or not isinstance(cat, discord.CategoryChannel): continue
        for ch in cat.channels:
            if not isinstance(ch, discord.TextChannel): continue
            if ch.id in ticket_sessions or ch.id in pending_channels: continue
            # Essai de trouver le "propriétaire" : premier non-bot non-admin avec accès
            humans = [m for m in g.members if not m.bot and ch.permissions_for(m).view_channel and not m.guild_permissions.administrator]
            owner_id = humans[0].id if humans else None
            pending_channels[ch.id] = {"guild_id": g.id, "channel": ch,
                                       "detected_at": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
                                       "owner_id": owner_id}
            total += 1
    if total: log("BOT", str(total) + " salon(s) existant(s) detecte(s)")

    # Scan des vocaux au démarrage — reprise de l'analyse pour membres déjà connectés
    now_boot = datetime.now(timezone.utc)
    vocal_count = 0
    for guild in client.guilds:
        for vc in guild.voice_channels:
            for member in vc.members:
                if member.bot: continue
                d = _act(guild.id, member.id)
                if not d.get("voice_joined"):  # pas déjà marqué
                    d["voice_joined"] = now_boot
                    d["last_seen"]    = now_boot
                    vocal_count += 1
    if vocal_count:
        log("BOT", f"Scan vocal démarrage : {vocal_count} membre(s) déjà en vocal → analyse reprise")
    # Charger le changelog manuel (stocké en BDD) avant la notification
    await load_changelog()
    # Notification de mise à jour dans les salons staff
    await send_update_notifications()

@client.event
async def on_guild_channel_create(channel):
    if not isinstance(channel, discord.TextChannel): return
    cfg = ticket_config.get(channel.guild.id)
    if not cfg or channel.category_id != cfg.get("category_id"): return
    # Attendre un court instant pour que les permissions soient appliquées par DraftBot
    await asyncio.sleep(2)
    humans = [m for m in channel.guild.members if not m.bot
              and channel.permissions_for(m).view_channel
              and not m.guild_permissions.administrator]
    owner_id = humans[0].id if humans else None
    pending_channels[channel.id] = {"guild_id": channel.guild.id, "channel": channel,
                                    "detected_at": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
                                    "owner_id": owner_id}
    log("BOT", "Salon detecte : #" + channel.name + (" — owner: " + str(owner_id) if owner_id else ""))

@client.event
async def on_guild_channel_delete(channel):
    """Nettoie les sessions et pending quand un salon ticket est supprimé manuellement."""
    if channel.id in ticket_sessions:
        s = ticket_sessions.pop(channel.id)
        log("BOT", "Salon #" + channel.name + " supprime -> session ticket #" + str(s.get("number", "?")) + " retiree.")
        await save_data()
    if channel.id in pending_channels:
        pending_channels.pop(channel.id)
        log("BOT", "Salon #" + channel.name + " supprime -> pending retire.")

@client.event
async def on_message(message):
    if message.author.bot: return
    if message.guild:
        # Ancien tracker (compat)
        activity_tracker[message.guild.id][message.author.id] = datetime.now(timezone.utc)
        # Nouveau système détaillé
        now = datetime.now(timezone.utc)
        d = _act(message.guild.id, message.author.id)
        d["msg_count"] = d.get("msg_count", 0) + 1
        d["msg_last"]  = now
        d["last_seen"] = now
    await handle_cmds(message)

@client.event
async def on_voice_state_update(member, before, after):
    """Tracker le temps vocal des membres pour le score d'activité."""
    if member.bot or not member.guild: return
    gid = member.guild.id
    now = datetime.now(timezone.utc)
    d   = _act(gid, member.id)
    # Quitte un vocal (ou change de salon)
    if before.channel and (not after.channel or before.channel != after.channel):
        if d.get("voice_joined"):
            elapsed = int((now - d["voice_joined"]).total_seconds())
            d["voice_seconds"] = d.get("voice_seconds", 0) + max(0, elapsed)
            d["last_seen"]     = now
            d["voice_joined"]  = None
    # Rejoint un vocal
    if after.channel and (not before.channel or before.channel != after.channel):
        d["voice_joined"] = now
        d["last_seen"]    = now

@client.event
async def on_raw_reaction_add(payload):
    if payload.user_id == client.user.id: return
    g = client.get_guild(payload.guild_id)
    if not g: return
    s = next((x for x in ticket_sessions.values()
               if x.get("staff_msg_id") == payload.message_id and x["guild_id"] == g.id and x["status"] == "pending"), None)
    if not s or payload.channel_id != s.get("staff_chan_id"): return
    m = g.get_member(payload.user_id)
    if not m or not is_staff(m, g.id): return
    e = str(payload.emoji)
    if   e == "✅": await do_action(g, s, "accepted", "", m)
    elif e == "❌": await do_action(g, s, "refused",  "", m)

# ── Commandes Discord ──────────────────────────────────────────────

# ══════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════════
#  MOTEUR MUSIQUE  v4 — Spotify + Deezer
#
#  ARCHITECTURE :
#  Spotify  → UX, recherche, autocomplete, métadonnées, playlists
#  Deezer   → source audio (preview 30s gratuit OU stream complet avec ARL)
#
#  Flux :
#  /play → Spotify (titre, artiste, durée, cover)
#        → Deezer search (titre + artiste) → deezer_id + preview_url
#        → _play_next() → stream_url (ARL si dispo) ou preview_url (30s)
#        → FFmpegPCMAudio(url_mp3_direct) → vc.play()
#
#  Sans DEEZER_ARL : previews 30s uniquement (signalé clairement)
#  Avec DEEZER_ARL  : lecture complète via pydeezer
#
#  Pas de YouTube, pas de yt_dlp, pas de cookies YouTube.
# ══════════════════════════════════════════════════════════════════

import shutil as _shutil, urllib.request as _urllib_req, json as _json_mod

# ── FFmpeg ─────────────────────────────────────────────────────────
FFMPEG_EXECUTABLE = _shutil.which("ffmpeg") or "/usr/bin/ffmpeg"
log("MUSIC", f"FFmpeg : {FFMPEG_EXECUTABLE}", flush=True)

# Options FFmpeg pour les streams MP3/FLAC Deezer (HTTP direct, pas chunked)
FFMPEG_BEFORE_OPTIONS = "-reconnect 1 -reconnect_delay_max 5"
FFMPEG_OPTIONS        = "-vn"

# ── Deezer config ──────────────────────────────────────────────────
DEEZER_ARL  = os.getenv("DEEZER_ARL", "")   # token ARL pour lecture complète (optionnel)
DEEZER_OK   = False                          # sera True si pydeezer dispo et ARL valide
_deezer_client = None                        # instance pydeezer (si disponible)

try:
    import pydeezer
    if DEEZER_ARL:
        _deezer_client = pydeezer.Deezer(arl=DEEZER_ARL)
        DEEZER_OK = True
        log("MUSIC", "pydeezer : OK ✔ (lecture complète disponible)", flush=True)
    else:
        log("MUSIC", "pydeezer : installé mais DEEZER_ARL absent → previews 30s uniquement", flush=True)
except ImportError:
    log("MUSIC", "pydeezer : non installé → previews 30s uniquement", flush=True)
except Exception as _e_dz:
    log("MUSIC", f"pydeezer : erreur init : {_e_dz} → previews 30s", flush=True)


# ── Deezer API (publique, sans auth) ──────────────────────────────
def _deezer_search(query: str) -> dict | None:
    """
    Cherche un titre sur Deezer via l'API publique.
    Retourne le premier résultat : {id, preview, cover, duration} ou None.
    L'API Deezer publique ne nécessite aucune clé.
    """
    try:
        import urllib.parse as _up
        url = "https://api.deezer.com/search?q=" + _up.quote(query) + "&limit=1&output=json"
        req = _urllib_req.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with _urllib_req.urlopen(req, timeout=10) as resp:
            data = _json_mod.loads(resp.read().decode())
        items = data.get("data") or []
        if not items:
            return None
        t = items[0]
        return {
            "deezer_id":   str(t.get("id", "")),
            "preview_url": t.get("preview", ""),   # MP3 30s direct, sans DRM
            "thumbnail":   (t.get("album") or {}).get("cover_medium") or (t.get("album") or {}).get("cover") or "",
            "duration":    t.get("duration", 0),
        }
    except Exception as e:
        log("MUSIC", f"Deezer search error : {e}", flush=True)
        return None


def _deezer_full_url(deezer_id: str) -> str | None:
    """Obtient l'URL du stream complet via pydeezer (nécessite DEEZER_ARL)."""
    if not DEEZER_OK or not _deezer_client:
        return None
    try:
        track_data = _deezer_client.get_track(deezer_id)
        download_url = track_data.get_url()
        return download_url
    except Exception as e:
        log("MUSIC", f"pydeezer get_url error : {e}", flush=True)
        return None


# ── État musique par serveur ──────────────────────────────────────
def _mstate(gid: int) -> dict:
    if gid not in music_state:
        music_state[gid] = {
            "queue":      [],
            "current":    None,
            "volume":     0.5,
            "np_channel": None,
        }
    return music_state[gid]

def _can_use_music(member: discord.Member, gid: int) -> tuple[bool, str]:
    cfg = tconf(gid)
    if cfg.get("music_locked", False):
        msg = cfg.get("music_lock_msg") or "🔒 Le système musique est temporairement indisponible."
        return False, msg
    if cfg.get("music_enabled", True):
        return True, ""
    role_id = cfg.get("music_role_id")
    if not role_id:
        return True, ""
    if any(r.id == role_id for r in member.roles):
        return True, ""
    role  = member.guild.get_role(role_id)
    rname = role.name if role else str(role_id)
    return False, f"🔒 Accès musique réservé au rôle **{rname}**."


# ── Classification de la query ────────────────────────────────────
def _classify_query(query: str) -> str:
    """
    'spotify_track' | 'spotify_album' | 'spotify_playlist' | 'text'
    YouTube et autres URLs ne sont plus supportés.
    """
    q = query.strip()
    if "spotify.com/track/"    in q: return "spotify_track"
    if "spotify.com/album/"    in q: return "spotify_album"
    if "spotify.com/playlist/" in q: return "spotify_playlist"
    return "text"


# ── Client Spotify ────────────────────────────────────────────────
def _sp_client():
    if not SPOTIPY_OK or not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
        return None
    try:
        return spotipy.Spotify(auth_manager=SpotifyClientCredentials(
            client_id=SPOTIFY_CLIENT_ID,
            client_secret=SPOTIFY_CLIENT_SECRET))
    except Exception as e:
        log("MUSIC", f"Erreur client Spotify : {e}", flush=True)
        return None


def _sp_track_info(t: dict, album_thumb: str = "") -> dict:
    """Extrait les infos d'un objet track Spotify → dict track normalisé."""
    name    = t.get("name") or "Titre inconnu"
    artists = t.get("artists") or []
    artist  = ", ".join(a.get("name", "") for a in artists) if artists else "Artiste inconnu"
    dur_s   = int((t.get("duration_ms") or 0) / 1000)
    images  = (t.get("album") or {}).get("images") or []
    thumb   = (images[0].get("url") if images else "") or album_thumb
    sp_url  = (t.get("external_urls") or {}).get("spotify") or ""
    return {
        "title":        name,
        "artist":       artist,
        "duration":     dur_s,
        "thumbnail":    thumb,
        "spotify_url":  sp_url,
        "deezer_id":    None,
        "preview_url":  None,
        "stream_url":   None,
        "requester":    "",
        "_fail_count":  0,
    }


# ── Résolution Spotify → liste de tracks ─────────────────────────
def _resolve_spotify_sync(query: str, qtype: str) -> list[dict]:
    """
    Spotify URL → liste de tracks normalisés.
    Chaque track a: title, artist, duration, thumbnail, spotify_url.
    deezer_id et stream_url sont résolus plus tard dans _play_next.
    """
    sp = _sp_client()
    if not sp:
        log("MUSIC", "Spotify : credentials manquants ou SPOTIPY non installé", flush=True)
        return []
    tracks = []
    try:
        if qtype == "spotify_track":
            t = sp.track(query)
            tracks = [_sp_track_info(t)]
            log("MUSIC", f"Spotify track : '{tracks[0]['title']}' — {tracks[0]['artist']}", flush=True)

        elif qtype == "spotify_album":
            album  = sp.album(query)
            images = (album.get("images") or [])
            thumb  = images[0].get("url") if images else ""
            items  = (sp.album_tracks(query, limit=50) or {}).get("items") or []
            for item in items:
                tr = _sp_track_info(item, album_thumb=thumb)
                tracks.append(tr)
            log("MUSIC", f"Spotify album : {len(tracks)} tracks — '{album.get('name','?')}'", flush=True)

        elif qtype == "spotify_playlist":
            pl      = sp.playlist(query, fields="name,images,tracks.items.track")
            images  = (pl.get("images") or [])
            pl_thumb = images[0].get("url") if images else ""
            items   = (pl.get("tracks") or {}).get("items") or []
            for item in items:
                t = item.get("track")
                if not t or not t.get("name"): continue
                tr = _sp_track_info(t, album_thumb=pl_thumb)
                tracks.append(tr)
            log("MUSIC", f"Spotify playlist : {len(tracks)} tracks — '{pl.get('name','?')}'", flush=True)

    except Exception as e:
        log("MUSIC", f"Erreur Spotify ({qtype}) : {e}", flush=True)
    return tracks


def _search_spotify_sync(query: str) -> list[dict]:
    """Recherche texte Spotify → premier résultat track."""
    sp = _sp_client()
    if not sp:
        return []
    try:
        results = sp.search(q=query, type="track", limit=1)
        items   = (results.get("tracks") or {}).get("items") or []
        if not items:
            log("MUSIC", f"Spotify search '{query[:50]}' : aucun résultat", flush=True)
            return []
        tr = _sp_track_info(items[0])
        log("MUSIC", f"Spotify search '{query[:50]}' → '{tr['title']}' — {tr['artist']}", flush=True)
        return [tr]
    except Exception as e:
        log("MUSIC", f"Spotify search error '{query[:50]}' : {e}", flush=True)
        return []


# ── Point d'entrée résolution ─────────────────────────────────────
async def _resolve_query(query: str) -> list[dict]:
    """Résout une query en liste de tracks Spotify."""
    qtype = _classify_query(query)
    log("MUSIC", f"Query='{query[:60]}' type={qtype}", flush=True)
    loop = asyncio.get_running_loop()

    if qtype in ("spotify_track", "spotify_album", "spotify_playlist"):
        return await loop.run_in_executor(None, _resolve_spotify_sync, query, qtype)

    # Recherche texte libre → Spotify search
    return await loop.run_in_executor(None, _search_spotify_sync, query)


# ── Résolution audio Deezer pour un track ─────────────────────────
def _resolve_deezer_for_track(track: dict) -> dict:
    """
    Cherche le track sur Deezer et peuple deezer_id + preview_url.
    Si DEEZER_ARL disponible, obtient aussi stream_url (lecture complète).
    Retourne le track modifié.
    """
    search_q = f"{track['title']} {track['artist']}"
    log("MUSIC", f"Deezer search : '{search_q[:60]}'", flush=True)
    dz = _deezer_search(search_q)
    if not dz:
        log("MUSIC", f"Deezer : aucun résultat pour '{search_q[:50]}'", flush=True)
        return track

    track["deezer_id"]   = dz["deezer_id"]
    track["preview_url"] = dz["preview_url"]
    if not track["thumbnail"] and dz.get("thumbnail"):
        track["thumbnail"] = dz["thumbnail"]
    if not track["duration"] and dz.get("duration"):
        track["duration"] = dz["duration"]

    # Tenter le stream complet si ARL disponible
    if DEEZER_OK and dz["deezer_id"]:
        full_url = _deezer_full_url(dz["deezer_id"])
        if full_url:
            track["stream_url"] = full_url
            log("MUSIC", f"Deezer stream complet ✔ (deezer_id={dz['deezer_id']})", flush=True)
            return track

    # Fallback : preview 30s
    if dz["preview_url"]:
        track["stream_url"] = dz["preview_url"]
        log("MUSIC", f"Deezer preview 30s ✔ url={dz['preview_url'][:60]}", flush=True)
    return track


# ── Utilitaires ───────────────────────────────────────────────────
def _fmt_duration(seconds: int) -> str:
    if not seconds: return "?"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


# ── Player ────────────────────────────────────────────────────────
async def _play_next(guild: discord.Guild):
    """
    Dépile et joue le prochain morceau via Deezer.
    Si pas de stream_url disponible → message clair à l'utilisateur.
    """
    st = _mstate(guild.id)
    vc = guild.voice_client

    if not vc or not vc.is_connected():
        log("MUSIC", f"[{guild.name}] _play_next : bot non connecté — abandon", flush=True)
        st["current"] = None
        return

    if not st["queue"]:
        log("MUSIC", f"[{guild.name}] File vide — déconnexion auto dans 60s", flush=True)
        st["current"] = None
        async def _auto_disconnect():
            await asyncio.sleep(60)
            vc2 = guild.voice_client
            if vc2 and vc2.is_connected() and not vc2.is_playing() and not vc2.is_paused():
                log("MUSIC", f"[{guild.name}] Déconnexion après inactivité", flush=True)
                await vc2.disconnect()
        asyncio.create_task(_auto_disconnect())
        return

    track = st["queue"].pop(0)
    st["current"] = track

    # Guard anti-boucle
    if track.get("_fail_count", 0) >= 3:
        log("MUSIC", f"[{guild.name}] Track échouée 3x → skip définitif", flush=True)
        await _play_next(guild)
        return

    log("MUSIC", f"[{guild.name}] ▶ '{track['title']}' — {track['artist']}", flush=True)

    # Étape 1 : résoudre le stream Deezer si pas encore fait
    if not track.get("stream_url"):
        log("MUSIC", f"[{guild.name}] Résolution Deezer...", flush=True)
        loop = asyncio.get_running_loop()
        track = await loop.run_in_executor(None, _resolve_deezer_for_track, track)
        st["current"] = track

    stream_url = track.get("stream_url")
    if not stream_url:
        title_disp = f"**{track['title']}** — {track['artist']}"
        log("MUSIC", f"[{guild.name}] Aucun stream Deezer pour {title_disp} — skip", flush=True)
        ch_id = st.get("np_channel")
        ch    = guild.get_channel(ch_id) if ch_id else None
        if ch:
            try:
                is_preview = not DEEZER_OK
                if is_preview:
                    msg = (f"⚠️ Impossible de lire **{track['title']}** — {track['artist']}\n"
                           f"_Deezer n'a pas trouvé ce morceau._\n"
                           f"Pour activer la lecture complète, configure `DEEZER_ARL`.")
                else:
                    msg = f"⚠️ Stream Deezer indisponible pour **{track['title']}** — skippé."
                await ch.send(msg)
            except Exception: pass
        track["_fail_count"] = track.get("_fail_count", 0) + 1
        await _play_next(guild)
        return

    # Vérifier si c'est un preview 30s et prévenir l'utilisateur
    is_preview = "preview" in stream_url or (not DEEZER_OK and not track.get("deezer_id") is None)
    if is_preview and not DEEZER_OK:
        log("MUSIC", f"[{guild.name}] Mode preview 30s (DEEZER_ARL absent)", flush=True)

    # Étape 2 : créer la source FFmpeg
    volume = st.get("volume", 0.5)
    log("MUSIC", f"[{guild.name}] Stream URL : {stream_url[:80]}", flush=True)
    log("MUSIC", f"[{guild.name}] FFmpeg : {FFMPEG_EXECUTABLE} | before={FFMPEG_BEFORE_OPTIONS}", flush=True)
    try:
        ffmpeg_source = discord.FFmpegPCMAudio(
            stream_url,
            executable=FFMPEG_EXECUTABLE,
            before_options=FFMPEG_BEFORE_OPTIONS,
            options=FFMPEG_OPTIONS,
            stderr=subprocess.PIPE,
        )
        source = discord.PCMVolumeTransformer(ffmpeg_source, volume=volume)
        log("MUSIC", f"[{guild.name}] Source FFmpeg créée ✔", flush=True)
    except Exception as e:
        import traceback as _tb3
        log("MUSIC", f"[{guild.name}] ERREUR FFmpegPCMAudio : {e}", flush=True)
        log("MUSIC", _tb3.format_exc(), flush=True)
        await _play_next(guild)
        return

    # Étape 3 : after callback
    import time as _time_after
    _after_ref = _time_after.time()
    track_title_for_after = f"{track.get('title','?')} — {track.get('artist','?')}"
    _ffmpeg_proc = getattr(ffmpeg_source, "_process", None)

    def _after(error):
        dur = _time_after.time() - _after_ref
        if error:
            log("MUSIC", f"[{guild.name}] after ERREUR ({dur:.1f}s) : {error}", flush=True)
            sterr = ""
            if _ffmpeg_proc and _ffmpeg_proc.stderr:
                try: sterr = _ffmpeg_proc.stderr.read(1000).decode("utf-8", errors="replace")
                except Exception: pass
            if sterr:
                log("MUSIC", f"[{guild.name}] FFmpeg stderr : {sterr[:300]}", flush=True)
            # retry
            if st.get("current") is track:
                track["_fail_count"] = track.get("_fail_count", 0) + 1
                if track["_fail_count"] < 3:
                    track.pop("stream_url", None)
                    st["queue"].insert(0, track)
                    st["current"] = None
        else:
            log("MUSIC", f"[{guild.name}] ✔ Lecture terminée ({dur:.1f}s) : {track_title_for_after}", flush=True)
            if dur < 3.0:
                log("MUSIC", f"[{guild.name}] ⚠ Très courte ({dur:.1f}s) — URL invalide ?", flush=True)
        try:
            lp = client.loop
            if lp and not lp.is_closed():
                asyncio.run_coroutine_threadsafe(_play_next(guild), lp)
        except Exception as ex:
            log("MUSIC", f"[{guild.name}] after exception : {ex}", flush=True)

    # Étape 4 : vc.play()
    log("MUSIC", f"[{guild.name}] Opus={discord.opus.is_loaded()} connected={vc.is_connected()} playing={vc.is_playing()}", flush=True)
    try:
        if vc.is_playing():
            vc.stop()
        vc.play(source, after=_after)
        await asyncio.sleep(0.5)
        playing = vc.is_playing()
        log("MUSIC", f"[{guild.name}] vc.play() → is_playing={playing}", flush=True)
    except discord.errors.ClientException as e:
        log("MUSIC", f"[{guild.name}] vc.play ClientException : {e}", flush=True)
        if "already" in str(e).lower():
            st["queue"].insert(0, track)
        else:
            await _play_next(guild)
        return
    except Exception as e:
        import traceback as _tb5
        log("MUSIC", f"[{guild.name}] vc.play EXCEPTION : {e}", flush=True)
        log("MUSIC", _tb5.format_exc(), flush=True)
        await _play_next(guild)
        return

    # Étape 5 : embed Now Playing
    ch_id = st.get("np_channel")
    ch    = guild.get_channel(ch_id) if ch_id else None
    if ch:
        try:
            await ch.send(embed=_now_playing_embed(track, guild))
            # Si preview 30s, avertir
            if is_preview and not DEEZER_OK:
                await ch.send(
                    "⏱️ **Aperçu 30s** — Pour la lecture complète, configure `DEEZER_ARL` "
                    "dans les variables d'environnement Render.",
                    delete_after=30
                )
        except Exception as e:
            log("MUSIC", f"[{guild.name}] Erreur embed NP : {e}", flush=True)


def _now_playing_embed(track: dict, guild: discord.Guild) -> discord.Embed:
    """Embed Now Playing enrichi avec métadonnées Spotify/Deezer."""
    title    = track.get("title")   or "Titre inconnu"
    artist   = track.get("artist")  or "Artiste inconnu"
    sp_url   = track.get("spotify_url") or ""
    thumbnail = track.get("thumbnail") or ""
    duration  = track.get("duration") or 0
    requester = track.get("requester") or "?"
    is_preview = not DEEZER_OK or ("preview" in (track.get("stream_url") or ""))

    desc = f"**[{title}]({sp_url})**" if sp_url else f"**{title}**"
    e = discord.Embed(
        title="🎵  Lecture en cours" + (" *(aperçu 30s)*" if is_preview else ""),
        description=desc,
        color=0x1DB954,
        timestamp=datetime.now(timezone.utc),
    )
    e.add_field(name="🎤  Artiste",      value=artist,                          inline=True)
    e.add_field(name="⏱  Durée",         value=_fmt_duration(duration),          inline=True)
    e.add_field(name="👤  Demandé par",  value=requester,                        inline=True)
    vc = guild.voice_client
    if vc and vc.channel:
        e.add_field(name="🔊  Vocal", value=vc.channel.name, inline=True)
    n = len(_mstate(guild.id)["queue"])
    if n:
        e.set_footer(text=f"{n} morceau(x) suivant(s) dans la file")
    if thumbnail:
        try: e.set_thumbnail(url=thumbnail)
        except Exception: pass
    return e


# ── Autocomplete /play — Spotify ──────────────────────────────────
async def _autocomplete_play(inter: discord.Interaction, current: str):
    """
    5 suggestions Spotify en temps réel.
    Retourne Choice(name='Titre — Artiste', value='spotify:track:ID').
    La value est une URI Spotify → _classify_query → 'spotify_track'.
    """
    if not current or len(current) < 2:
        return []
    if not SPOTIPY_OK or not SPOTIFY_CLIENT_ID:
        return []
    loop = asyncio.get_running_loop()

    def _search():
        sp = _sp_client()
        if not sp: return []
        try:
            res   = sp.search(q=current, type="track", limit=5)
            items = (res.get("tracks") or {}).get("items") or []
            out   = []
            for t in items:
                name    = t.get("name", "?")[:80]
                artists = t.get("artists") or []
                artist  = artists[0].get("name", "?") if artists else "?"
                label   = f"{name} — {artist}"[:100]
                # Construire une URL Spotify complète (reconnue par _classify_query)
                sp_id   = t.get("id", "")
                value   = f"https://open.spotify.com/track/{sp_id}"
                if sp_id:
                    out.append((label, value))
            log("MUSIC", f"Autocomplete '{current[:30]}' → {len(out)} suggestions Spotify", flush=True)
            return out
        except Exception as ex:
            log("MUSIC", f"Autocomplete erreur : {ex}", flush=True)
            return []

    try:
        results = await asyncio.wait_for(
            loop.run_in_executor(None, _search),
            timeout=2.8
        )
        return [app_commands.Choice(name=t, value=u) for t, u in results if u][:5]
    except asyncio.TimeoutError:
        log("MUSIC", f"Autocomplete TIMEOUT pour '{current[:30]}'", flush=True)
        return []
    except Exception as ex:
        log("MUSIC", f"Autocomplete exception : {ex}", flush=True)
        return []



#  MENU DISCORD STAFF — DESIGN MODERNE
# ══════════════════════════════════════════════════════════════════


def _e_ok(title, desc=""):
    return discord.Embed(title=title, description=desc or None, color=C_GREEN)

def _e_err(title, desc=""):
    return discord.Embed(title=title, description=desc or None, color=C_RED)

def _e_warn(title, desc=""):
    return discord.Embed(title=title, description=desc or None, color=C_AMBER)

def _e_empty(title, desc=""):
    return discord.Embed(title=title, description="*" + desc + "*" if desc else None, color=C_GREY)


# ══════════════════════════════════════════════════════════════════
#  SYSTÈME D'ACTIVITÉ MEMBRES
#  Données en RAM — tracke messages + temps vocal par membre/serveur
#  Rôles exemptés → visibles console uniquement (hidden publiquement)
# ══════════════════════════════════════════════════════════════════

# activity_data[guild_id][member_id] = {
#   "msg_count": int, "msg_last": datetime|None,
#   "voice_seconds": int, "voice_joined": datetime|None,
#   "last_seen": datetime|None
# }
activity_data: dict = {}


def _act(gid: int, mid: int) -> dict:
    """Retourne (et initialise si besoin) le dict d'activité d'un membre."""
    activity_data.setdefault(gid, {})
    activity_data[gid].setdefault(mid, {
        "msg_count":     0,
        "msg_last":      None,
        "voice_seconds": 0,
        "voice_joined":  None,
        "last_seen":     None,
    })
    return activity_data[gid][mid]


def _voice_seconds_current(gid: int, mid: int) -> int:
    """Secondes vocales en cours (session ouverte non encore fermée)."""
    d = activity_data.get(gid, {}).get(mid, {})
    joined = d.get("voice_joined")
    if joined:
        return int((datetime.now(timezone.utc) - joined).total_seconds())
    return 0


def _total_voice(gid: int, mid: int) -> int:
    return activity_data.get(gid, {}).get(mid, {}).get("voice_seconds", 0) \
           + _voice_seconds_current(gid, mid)


def _inactivity_score(gid: int, mid: int) -> int:
    """
    Score d'inactivité de 0 (très actif) à 100 (totalement inactif).
    Basé sur le temps écoulé depuis la dernière activité, pondéré
    par les messages et le temps vocal.
    """
    d = activity_data.get(gid, {}).get(mid, {})
    last = d.get("last_seen")
    if not last:
        return 95  # jamais vu depuis le démarrage du bot

    days = (datetime.now(timezone.utc) - last).total_seconds() / 86400
    msgs  = d.get("msg_count", 0)
    voice = _total_voice(gid, mid) / 3600  # en heures

    # Base inactivité selon les jours
    base = min(days * 10, 80)

    # Bonus activité (réduit le score)
    bonus = min(msgs * 0.5 + voice * 5, 40)

    return max(0, min(100, int(base - bonus)))


def _score_label(score: int) -> tuple[str, str]:
    """Retourne (emoji, label) selon le score."""
    if score <= 15:  return "🟢", "Très actif"
    if score <= 35:  return "🟡", "Actif"
    if score <= 55:  return "🟠", "Peu actif"
    if score <= 75:  return "🔴", "Inactif"
    return "⚫", "Très inactif"


def _is_exempt(member: discord.Member, gid: int) -> bool:
    """True si le membre a un rôle exempté d'analyse publique."""
    exempt_ids = tconf(gid).get("activity_exempt_role_ids", [])
    if not exempt_ids:
        return False
    return any(r.id in exempt_ids for r in member.roles)


def _fmt_seconds(s: int) -> str:
    if s < 60:   return f"{s}s"
    if s < 3600: return f"{s//60}m {s%60:02d}s"
    h, r = divmod(s, 3600)
    return f"{h}h {r//60:02d}m"


def _fmt_last_seen(dt: datetime | None) -> str:
    if not dt: return "Jamais (depuis le démarrage)"
    delta = datetime.now(timezone.utc) - dt
    s = int(delta.total_seconds())
    if s < 60:   return "Il y a quelques secondes"
    if s < 3600: return f"Il y a {s//60} min"
    if s < 86400: return f"Il y a {s//3600}h"
    return f"Il y a {s//86400} jour(s)"


# ══════════════════════════════════════════════════════════════════
#  PAGINATION UNIVERSELLE
#  PaginatedView : view réutilisable pour tout contenu long
#  PAGE_SIZE = 10 lignes dans embed, 24 items max dans le Select
# ══════════════════════════════════════════════════════════════════

PAGE_ITEMS = 10   # items par page dans les embeds listés
SEL_ITEMS  = 24   # max options dans un Select Discord (limite API)


def _paginate(items: list, page: int, per_page: int = PAGE_ITEMS) -> tuple[list, int, bool, bool]:
    """Retourne (slice, total_pages, has_prev, has_next)."""
    total = max(1, (len(items) + per_page - 1) // per_page)
    page  = max(0, min(page, total - 1))
    start = page * per_page
    return items[start:start + per_page], total, page > 0, start + per_page < len(items)


def _page_footer(page: int, total: int, count: int, label: str = "élément(s)") -> str:
    return f"Page {page+1}/{total}  •  {count} {label} au total"



def _member_activity_embed(member: discord.Member, gid: int) -> discord.Embed:
    """Embed de rapport d'activité pour un membre spécifique."""
    d     = activity_data.get(gid, {}).get(member.id, {})
    score = _inactivity_score(gid, member.id)
    emoji, label = _score_label(score)
    msgs  = d.get("msg_count", 0)
    voice = _total_voice(gid, member.id)
    last  = d.get("last_seen")
    exempt = _is_exempt(member, gid)

    # Couleur selon le score
    color = [0x57F287, 0xFEE75C, 0xF0A500, 0xED4245, 0x36393F][min(score//20, 4)]

    e = discord.Embed(
        description=(
            f"### {emoji}  Rapport d'activité\n"
            f"{'*(Membre exempté — données masquées publiquement)*' if exempt else ''}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        ),
        color=color,
        timestamp=datetime.now(timezone.utc)
    )
    e.set_author(
        name=f"{member.display_name}",
        icon_url=member.display_avatar.url if member.display_avatar else None
    )
    if member.display_avatar:
        e.set_thumbnail(url=member.display_avatar.url)

    # Score principal
    bar_filled = round(score / 10)
    bar = "█" * bar_filled + "░" * (10 - bar_filled)
    e.add_field(
        name="📊  Score d'inactivité",
        value=f"`{bar}` **{score}/100** — {label}",
        inline=False
    )

    # Stats
    e.add_field(
        name="💬  Messages",
        value=f"> **{msgs}** message(s)\n> {_fmt_last_seen(d.get('msg_last'))}",
        inline=True
    )
    e.add_field(
        name="🔊  Temps vocal",
        value=f"> **{_fmt_seconds(voice)}**\n> {'En vocal 🟢' if d.get('voice_joined') else 'Hors vocal'}",
        inline=True
    )
    e.add_field(
        name="👁️  Dernière activité",
        value=f"> {_fmt_last_seen(last)}",
        inline=True
    )

    # Tous les rôles — paginés si besoin (limite embed 1024 chars)
    all_roles = [r for r in reversed(member.roles) if r.name != "@everyone"]
    if all_roles:
        # Construire la chaîne de mentions en respectant la limite Discord
        parts, buf = [], ""
        for r in all_roles:
            mention = r.mention
            if len(buf) + len(mention) + 1 > 950:
                parts.append(buf.rstrip())
                buf = ""
            buf += mention + " "
        if buf.strip(): parts.append(buf.strip())
        for i, part in enumerate(parts):
            e.add_field(
                name=(f"🏷️  Rôles (suite) — {len(all_roles)} au total" if i > 0 else f"🏷️  Rôles — {len(all_roles)} au total"),
                value=part,
                inline=False
            )

    # Bouton Actualiser dans le footer (via timestamp qui change)
    _now_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
    e.set_footer(text=f"🔄 Actualisé à {_now_str} UTC  •  {member.guild.name}")
    return e


# ══════════════════════════════════════════════════════════════════
#  MENU DISCORD — PANEL ADMIN COMPLET
# ══════════════════════════════════════════════════════════════════

async def send_staff_menu(channel, guild, invoker=None):
    if invoker and not is_staff(invoker, guild.id):
        await channel.send(embed=_e_err("🚫  Accès refusé", "Réservé au staff."))
        return
    if maintenance_mode:
        if invoker:
            await channel.send(embed=_e_warn("🔧  Maintenance", "Panel indisponible pendant la maintenance."))
        return
    cfg = tconf(guild.id)
    if cfg.get("menu_locked"):
        if invoker:
            await channel.send(embed=_e_warn("🔒  Panel verrouillé", "Contacte un administrateur."))
        return
    await _delete_active_menu(guild.id, channel=channel)
    msg = await channel.send(embed=_build_home_embed(guild), view=DashboardView(guild))
    active_menu_msgs[guild.id] = (channel.id, msg.id)


async def send_staff_menu_inter(inter: discord.Interaction, guild):
    if maintenance_mode:
        e = discord.Embed(title="🔧  Bot en maintenance",
                          description="Le panel est indisponible pendant la maintenance.",
                          color=0xE67E22)
        if inter.response.is_done(): await inter.followup.send(embed=e, ephemeral=True)
        else: await inter.response.send_message(embed=e, ephemeral=True)
        return
    cfg = tconf(guild.id)
    if cfg.get("menu_locked"):
        e = _e_warn("🔒  Panel verrouillé", "Contacte un administrateur.")
        if inter.response.is_done(): await inter.followup.send(embed=e, ephemeral=True)
        else: await inter.response.send_message(embed=e, ephemeral=True)
        return
    await _delete_active_menu(guild.id, channel=inter.channel)
    view = DashboardView(guild)
    if inter.response.is_done():
        msg = await inter.followup.send(embed=_build_home_embed(guild), view=view, wait=True)
    else:
        await inter.response.send_message(embed=_build_home_embed(guild), view=view)
        msg = await inter.original_response()
    active_menu_msgs[guild.id] = (inter.channel.id, msg.id)


# ── Embeds par section ────────────────────────────────────────────

def _build_home_embed(guild: discord.Guild) -> discord.Embed:
    cfg    = tconf(guild.id)
    cat    = guild.get_channel(cfg.get("category_id") or 0)
    sch    = guild.get_channel(cfg.get("staff_channel_id") or 0)
    sro    = guild.get_role(cfg.get("staff_role_id") or 0)
    humans = [m for m in guild.members if not m.bot]
    online = [m for m in humans if m.status != discord.Status.offline]
    bots   = [m for m in guild.members if m.bot]
    ot     = [s for s in ticket_sessions.values()
               if s["guild_id"] == guild.id and s["status"] in ("open", "pending")]
    ct     = [s for s in ticket_sessions.values()
               if s["guild_id"] == guild.id and s["status"] in ("accepted", "refused", "closed")]
    ok_cfg, _ = is_configured(guild.id)
    anon_on   = cfg.get("anon_enabled", True)

    status = "✅  Opérationnel"
    if maintenance_mode: status = "🔧  Maintenance active"
    elif not ok_cfg:     status = "⚠️  Configuration incomplète"

    e = discord.Embed(
        description=(
            f"### 🏠  Panel d'administration — {guild.name}\n"
            f"*{status}*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        ),
        color=0x5865F2,
        timestamp=datetime.now(timezone.utc)
    )
    if guild.icon:
        e.set_thumbnail(url=guild.icon.url)

    e.add_field(
        name="👥  Communauté",
        value=(
            f"> 👤  **{len(humans)}** membres\n"
            f"> 🟢  **{len(online)}** en ligne\n"
            f"> 🤖  **{len(bots)}** bots"
        ),
        inline=True
    )
    e.add_field(
        name="🛡️  Staff",
        value=(
            f"> 📣  {sch.mention if sch else '`Non défini`'}\n"
            f"> 🛡  {sro.mention if sro else '`Non défini`'}\n"
            f"> 📁  `{cat.name if cat else 'Non définie'}`"
        ),
        inline=True
    )
    e.add_field(
        name="🎫  Tickets",
        value=(
            f"> 🟡  **{len(ot)}** en cours\n"
            f"> ✅  **{len(ct)}** traités\n"
            f"> 📝  **{len(cfg.get('types', {}))}** types"
        ),
        inline=True
    )
    e.add_field(
        name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        value=(
            f"💬 Messages anonymes : {'`Activés`' if anon_on else '`Désactivés`'}  •  "
            f"📊 Activité : `{sum(len(v) for v in activity_data.get(guild.id, {}).values() if isinstance(v, dict))} membre(s) suivis`"
            if False else  # simplifié
            f"💬 Anonymes : {'`ON`' if anon_on else '`OFF`'}  •  "
            f"📊 Membres suivis : `{len(activity_data.get(guild.id, {}))}`"
        ),
        inline=False
    )
    alerts = []
    if maintenance_mode:       alerts.append("🔧 Maintenance")
    if cfg.get("menu_locked"): alerts.append("🔒 Verrouillé")
    if not ok_cfg:             alerts.append("⚠️ Config incomplète")
    footer = "  •  ".join(alerts) if alerts else "Panel Admin"
    e.set_footer(text=f"{footer}  •  {len(humans)} membres  •  Naviguer via les menus ci-dessous")
    return e


def _build_embed_server(guild: discord.Guild) -> discord.Embed:
    humans = [m for m in guild.members if not m.bot]
    online = [m for m in humans if m.status != discord.Status.offline]
    idle   = [m for m in humans if m.status == discord.Status.idle]
    dnd    = [m for m in humans if m.status == discord.Status.dnd]
    bots   = [m for m in guild.members if m.bot]
    txt_ch = [c for c in guild.channels if isinstance(c, discord.TextChannel)]
    voc_ch = [c for c in guild.channels if isinstance(c, discord.VoiceChannel)]

    e = discord.Embed(
        description=(
            f"### 🌐  Informations du serveur\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        ),
        color=0x1ABC9C,
        timestamp=datetime.now(timezone.utc)
    )
    if guild.icon:
        e.set_thumbnail(url=guild.icon.url)

    e.add_field(
        name="🆔  Identité",
        value=(
            f"> **{guild.name}**\n"
            f"> 🆔  `{guild.id}`\n"
            f"> 👑  {guild.owner.mention if guild.owner else '`Inconnu`'}\n"
            f"> 📅  Créé <t:{int(guild.created_at.timestamp())}:D>"
        ),
        inline=True
    )
    e.add_field(
        name="👥  Membres",
        value=(
            f"> 👤  **{len(humans)}** humains\n"
            f"> 🟢  **{len(online)}** en ligne\n"
            f"> 🟡  **{len(idle)}** inactif\n"
            f"> 🔴  **{len(dnd)}** ne pas déranger\n"
            f"> 🤖  **{len(bots)}** bots"
        ),
        inline=True
    )
    e.add_field(
        name="📡  Salons & Rôles",
        value=(
            f"> 💬  **{len(txt_ch)}** texte\n"
            f"> 🔊  **{len(voc_ch)}** vocal\n"
            f"> 📂  **{len(guild.categories)}** catégories\n"
            f"> 🏷️   **{len(guild.roles)-1}** rôles"
        ),
        inline=True
    )
    # Top rôles
    top_roles = sorted([r for r in guild.roles if r.name != "@everyone"],
                        key=lambda r: r.position, reverse=True)[:6]
    if top_roles:
        e.add_field(
            name="🏆  Rôles principaux",
            value=" ".join(r.mention for r in top_roles),
            inline=False
        )
    return e


def _build_embed_members(guild: discord.Guild, page: int = 0) -> discord.Embed:
    humans  = sorted([m for m in guild.members if not m.bot],
                     key=lambda m: (m.status == discord.Status.offline, m.display_name.lower()))
    online  = [m for m in humans if m.status != discord.Status.offline]
    si      = {discord.Status.online: "🟢", discord.Status.idle: "🟡",
                discord.Status.dnd: "🔴", discord.Status.offline: "⚫"}
    chunk, total_pages, has_prev, has_next = _paginate(humans, page, 15)
    e = discord.Embed(
        description=(
            f"### 👥  Membres du serveur\n"
            f"`{len(humans)}` membre(s)  •  `{len(online)}` en ligne\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        ),
        color=0x57F287,
        timestamp=datetime.now(timezone.utc)
    )
    lines    = [f"{si.get(m.status,'⚫')} **{m.display_name}**" for m in chunk]
    col_size = max(1, (len(lines) + 2) // 3)
    for ci in range(3):
        col = lines[ci * col_size:(ci + 1) * col_size]
        if col: e.add_field(name="\u200b", value="\n".join(col), inline=True)
    e.set_footer(text=_page_footer(page, total_pages, len(humans), "membre(s)"))
    return e


def _build_embed_bots(guild: discord.Guild, page: int = 0) -> discord.Embed:
    bots  = sorted([m for m in guild.members if m.bot], key=lambda m: m.display_name.lower())
    si    = {discord.Status.online: "🟢", discord.Status.idle: "🟡",
              discord.Status.dnd: "🔴", discord.Status.offline: "⚫"}
    chunk, total_pages, has_prev, has_next = _paginate(bots, page, 12)
    e = discord.Embed(
        description=(
            f"### 🤖  Bots du serveur\n"
            f"`{len(bots)}` bot(s)\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        ),
        color=0x5865F2,
        timestamp=datetime.now(timezone.utc)
    )
    for b in chunk:
        e.add_field(
            name=f"{si.get(b.status,'⚫')} {b.display_name}",
            value=f"> 🆔 `{b.id}`",
            inline=True
        )
    e.set_footer(text=_page_footer(page, total_pages, len(bots), "bot(s)"))
    return e


def _build_embed_activity(guild: discord.Guild, show_exempt: bool = False) -> discord.Embed:
    """Embed liste d'activité — tous les membres triés par score d'inactivité."""
    cfg    = tconf(guild.id)
    humans = [m for m in guild.members if not m.bot]

    rows = []
    for m in humans:
        exempt = _is_exempt(m, guild.id)
        if exempt and not show_exempt:
            continue
        score = _inactivity_score(guild.id, m.id)
        rows.append((score, m, exempt))
    rows.sort(key=lambda x: x[0], reverse=True)  # plus inactifs en premier

    e = discord.Embed(
        description=(
            f"### 📊  Activité des membres\n"
            f"`{len(rows)}` membre(s) analysés\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        ),
        color=0xF0A500,
        timestamp=datetime.now(timezone.utc)
    )

def _build_embed_activity(guild: discord.Guild, show_exempt: bool = False, page: int = 0) -> discord.Embed:
    """Embed paginé d'activité — triés du plus inactif au plus actif."""
    humans = [m for m in guild.members if not m.bot]
    rows = []
    for m in humans:
        exempt = _is_exempt(m, guild.id)
        if exempt and not show_exempt:
            continue
        score = _inactivity_score(guild.id, m.id)
        rows.append((score, m, exempt))
    rows.sort(key=lambda x: x[0], reverse=True)
    chunk, total_pages, has_prev, has_next = _paginate(rows, page, 10)
    e = discord.Embed(
        description=(
            f"### 📊  Activité des membres\n"
            f"`{len(rows)}` membre(s) analysés\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        ),
        color=0xF0A500,
        timestamp=datetime.now(timezone.utc)
    )
    for score, m, exempt in chunk:
        emoji, label = _score_label(score)
        voice = _fmt_seconds(_total_voice(guild.id, m.id))
        msgs  = activity_data.get(guild.id, {}).get(m.id, {}).get("msg_count", 0)
        tag   = " *(exempté)*" if exempt else ""
        bar   = "█" * round(score/10) + "░" * (10 - round(score/10))
        e.add_field(
            name=f"{emoji}  {m.display_name}{tag}",
            value=f"> `{bar}` **{score}** — {label}\n> 💬 {msgs} msgs  🔊 {voice}",
            inline=False
        )
    e.set_footer(text=_page_footer(page, total_pages, len(rows), "membre(s)") +
                 "  •  /activite @membre pour le détail")
    return e


def _build_embed_config_staff(guild: discord.Guild) -> discord.Embed:
    cfg  = tconf(guild.id)
    sch  = guild.get_channel(cfg.get("staff_channel_id") or 0)
    sro  = guild.get_role(cfg.get("staff_role_id") or 0)
    anon = cfg.get("anon_enabled", True)
    exempt_ids = cfg.get("activity_exempt_role_ids", [])
    exempt_roles = [guild.get_role(rid) for rid in exempt_ids if guild.get_role(rid)]
    ok_cfg, msg_cfg = is_configured(guild.id)

    e = discord.Embed(
        description=(
            f"### 🛡️  Configuration — Staff & Permissions\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        ),
        color=0x5865F2,
        timestamp=datetime.now(timezone.utc)
    )
    e.add_field(
        name="📣  Salon des notifications",
        value=sch.mention if sch else "`❌ Non défini`",
        inline=True
    )
    e.add_field(
        name="🛡️  Rôle staff",
        value=sro.mention if sro else "`❌ Non défini`",
        inline=True
    )
    e.add_field(
        name="💬  Messages anonymes",
        value="`✅ Activés`" if anon else "`🔇 Désactivés`",
        inline=True
    )
    e.add_field(
        name="🙈  Rôles exemptés (activité)",
        value=(" ".join(r.mention for r in exempt_roles)
               if exempt_roles else "`Aucun rôle exempté`"),
        inline=False
    )
    if not ok_cfg:
        e.add_field(name="⚠️  Action requise", value=f"> {msg_cfg}", inline=False)
    e.set_footer(text="Utilise les menus ci-dessous pour modifier.")
    return e


def _build_embed_config_tickets(guild: discord.Guild) -> discord.Embed:
    cfg = tconf(guild.id)
    cat = guild.get_channel(cfg.get("category_id") or 0)
    ot  = [s for s in ticket_sessions.values()
            if s["guild_id"] == guild.id and s["status"] in ("open", "pending")]
    ct  = [s for s in ticket_sessions.values()
            if s["guild_id"] == guild.id and s["status"] in ("accepted", "refused", "closed")]

    e = discord.Embed(
        description=(
            f"### 🎫  Configuration — Tickets\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        ),
        color=0x9B59B6,
        timestamp=datetime.now(timezone.utc)
    )
    e.add_field(
        name="📁  Catégorie",
        value=f"`{cat.name}`" if cat else "`❌ Non définie`",
        inline=True
    )
    e.add_field(
        name="🔢  Compteur",
        value=f"`#{str(cfg['counter']).zfill(4)}`",
        inline=True
    )
    e.add_field(
        name="📝  Types",
        value=f"`{len(cfg.get('types', {}))}` type(s)",
        inline=True
    )
    e.add_field(
        name="📊  Statistiques",
        value=(
            f"> 🟡  **{len(ot)}** tickets en cours\n"
            f"> ✅  **{len(ct)}** traités"
        ),
        inline=False
    )
    if cfg.get("types"):
        types_str = "  ".join(
            f"{t.get('emoji','🎫')} `{t.get('label','?')}`"
            for t in list(cfg["types"].values())[:6]
        )
        e.add_field(name="🏷️  Types configurés", value=types_str, inline=False)
    e.set_footer(text="Utilise les menus ci-dessous pour modifier.")
    return e



class MembersPageView(discord.ui.View):
    """View paginée pour la section Membres — boutons ◀ ▶ + select profil."""
    def __init__(self, guild: discord.Guild, page: int = 0):
        super().__init__(timeout=300)
        self.guild = guild
        self.page  = page
        si = {discord.Status.online: "🟢", discord.Status.idle: "🟡",
               discord.Status.dnd: "🔴", discord.Status.offline: "⚫"}
        humans = sorted([m for m in guild.members if not m.bot],
                        key=lambda m: (m.status == discord.Status.offline, m.display_name.lower()))
        _, total_pages, _, _ = _paginate(humans, page, SEL_ITEMS)
        self._total = total_pages
        start = page * SEL_ITEMS
        chunk = humans[start:start + SEL_ITEMS]
        if chunk:
            opts = [discord.SelectOption(
                label=f"{si.get(m.status,'⚫')} {m.display_name[:45]}",
                description=f"ID: {m.id}",
                value=str(m.id)
            ) for m in chunk]
            sel = discord.ui.Select(
                placeholder=f"👤  Profil membre… (p.{page+1}/{total_pages})",
                options=opts, row=0
            )
            sel.callback = self._on_select
            self.add_item(sel)
        btn_prev = discord.ui.Button(label="◀  Précédent", style=discord.ButtonStyle.secondary,
                                     disabled=(page == 0), row=1)
        btn_prev.callback = self._prev
        self.add_item(btn_prev)
        btn_next = discord.ui.Button(label="Suivant  ▶", style=discord.ButtonStyle.secondary,
                                     disabled=(page >= total_pages - 1), row=1)
        btn_next.callback = self._next
        self.add_item(btn_next)

    async def _on_select(self, inter: discord.Interaction):
        mem = self.guild.get_member(int(inter.data["values"][0]))
        if not mem:
            await inter.response.send_message(embed=_e_err("❌", "Membre introuvable."), ephemeral=True); return
        si  = {discord.Status.online: "🟢 En ligne", discord.Status.idle: "🟡 AFK",
                discord.Status.dnd: "🔴 DND", discord.Status.offline: "⚫ Hors ligne"}
        col = mem.top_role.color if mem.top_role and mem.top_role.color.value else discord.Color.default()
        e   = discord.Embed(title=f"👤  {mem.display_name}",
                            description=f"Membre depuis <t:{int(mem.joined_at.timestamp())}:D>", color=col)
        if mem.display_avatar: e.set_thumbnail(url=mem.display_avatar.url)
        e.add_field(name="📊  Statut",   value=si.get(mem.status, "⚫"), inline=True)
        e.add_field(name="🏷  Top rôle", value=mem.top_role.mention if mem.top_role else "—", inline=True)
        e.add_field(name="🆔  ID",       value=f"`{mem.id}`", inline=True)
        # Activité
        score = _inactivity_score(self.guild.id, mem.id)
        emoji_a, label_a = _score_label(score)
        e.add_field(name="📊  Activité",
                    value=f"{emoji_a} Score {score}/100 — {label_a}", inline=True)
        roles_txt = " ".join(r.mention for r in reversed(mem.roles) if r.name != "@everyone")[:512] or "*Aucun*"
        e.add_field(name="📋  Rôles", value=roles_txt, inline=False)
        await inter.response.send_message(embed=e, view=MemberModView(self.guild, mem), ephemeral=True)

    async def _prev(self, inter: discord.Interaction):
        embed = _build_embed_members(self.guild, self.page - 1)
        await inter.response.edit_message(embed=embed, view=MembersPageView(self.guild, self.page - 1))

    async def _next(self, inter: discord.Interaction):
        embed = _build_embed_members(self.guild, self.page + 1)
        await inter.response.edit_message(embed=embed, view=MembersPageView(self.guild, self.page + 1))


class BotsPageView(discord.ui.View):
    """View paginée pour la section Bots."""
    def __init__(self, guild: discord.Guild, page: int = 0):
        super().__init__(timeout=300)
        self.guild = guild
        self.page  = page
        bots = sorted([m for m in guild.members if m.bot], key=lambda m: m.display_name.lower())
        _, total_pages, _, _ = _paginate(bots, page, 12)
        btn_prev = discord.ui.Button(label="◀  Précédent", style=discord.ButtonStyle.secondary,
                                     disabled=(page == 0), row=0)
        btn_prev.callback = self._prev
        self.add_item(btn_prev)
        btn_next = discord.ui.Button(label="Suivant  ▶", style=discord.ButtonStyle.secondary,
                                     disabled=(page >= total_pages - 1), row=0)
        btn_next.callback = self._next
        self.add_item(btn_next)

    async def _prev(self, inter: discord.Interaction):
        embed = _build_embed_bots(self.guild, self.page - 1)
        await inter.response.edit_message(embed=embed, view=BotsPageView(self.guild, self.page - 1))

    async def _next(self, inter: discord.Interaction):
        embed = _build_embed_bots(self.guild, self.page + 1)
        await inter.response.edit_message(embed=embed, view=BotsPageView(self.guild, self.page + 1))

# ══════════════════════════════════════════════════════════════════
#  DashboardView — 3 menus déroulants + boutons
# ══════════════════════════════════════════════════════════════════
class DashboardView(discord.ui.View):
    def __init__(self, guild: discord.Guild, active: str = "home"):
        super().__init__(timeout=600)
        self.guild  = guild
        self.active = active

        # ── Menu 1 : Navigation principale ───────────────────────
        nav = discord.ui.Select(placeholder="📂  Navigation…", row=0, options=[
            discord.SelectOption(label="🏠  Accueil",            value="home",     description="Vue d'ensemble",                   default=active=="home"),
            discord.SelectOption(label="🎫  Tickets",            value="tickets",  description="Gérer les tickets en cours",        default=active=="tickets"),
            discord.SelectOption(label="📥  Salons en attente",  value="pending",  description="Lancer les questions DraftBot",     default=active=="pending"),
            discord.SelectOption(label="📝  Types de tickets",   value="types",    description="Créer et gérer les types",          default=active=="types"),
        ])
        nav.callback = self._nav
        self.add_item(nav)

        # ── Menu 2 : Serveur & Membres ────────────────────────────
        srv = discord.ui.Select(placeholder="🌐  Serveur & Membres…", row=1, options=[
            discord.SelectOption(label="👥  Membres",    value="members",   description="Liste des membres avec statut",  default=active=="members"),
            discord.SelectOption(label="🤖  Bots",       value="bots",      description="Bots présents sur le serveur",   default=active=="bots"),
            discord.SelectOption(label="📊  Activité",   value="activity",  description="Rapports d'activité membres",    default=active=="activity"),
            discord.SelectOption(label="🌐  Infos serveur", value="server", description="Statistiques du serveur",        default=active=="server"),
        ])
        srv.callback = self._nav
        self.add_item(srv)

        # ── Menu 3 : Configuration ────────────────────────────────
        cfg_sel = discord.ui.Select(placeholder="⚙️  Configuration…", row=2, options=[
            discord.SelectOption(label="🛡️  Staff & Permissions",   value="cfg_staff",   description="Salon, rôle staff, anonymes",   default=active=="cfg_staff"),
            discord.SelectOption(label="🎫  Système de tickets",     value="cfg_tickets", description="Catégorie, compteur, types",     default=active=="cfg_tickets"),
        ])
        cfg_sel.callback = self._nav
        self.add_item(cfg_sel)

        # ── Boutons ───────────────────────────────────────────────
        btn_refresh = discord.ui.Button(label="Actualiser", style=discord.ButtonStyle.secondary, emoji="🔄", row=3)
        btn_refresh.callback = self._refresh
        self.add_item(btn_refresh)

        btn_close = discord.ui.Button(label="Fermer", style=discord.ButtonStyle.danger, emoji="✖️", row=3)
        btn_close.callback = self._close
        self.add_item(btn_close)

    def _guard(self, inter):
        if maintenance_mode:               return False, "🔧  Bot en maintenance."
        if not is_staff(inter.user, self.guild.id): return False, "🚫  Réservé au staff."
        if tconf(self.guild.id).get("menu_locked"): return False, "🔒  Panel verrouillé."
        return True, ""

    async def _nav(self, inter: discord.Interaction):
        ok2, reason = self._guard(inter)
        if not ok2:
            await inter.response.send_message(embed=_e_warn("⚠️", reason), ephemeral=True); return
        val = inter.data["values"][0]
        g   = self.guild
        cfg = tconf(g.id)

        # Sections éphémères (sous-vues séparées)
        if val == "tickets":
            ss = [s for s in ticket_sessions.values()
                  if s["guild_id"] == g.id and s["status"] in ("open", "pending")]
            e = discord.Embed(
                description=(
                    f"### 🎫  Tickets en cours\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
                ),
                color=0xF0A500
            )
            if ss:
                icons = {"open": "🟡", "pending": "⏳"}
                for s in sorted(ss, key=lambda x: x["number"], reverse=True)[:6]:
                    mem = g.get_member(s["user_id"])
                    mn  = mem.display_name if mem else str(s["user_id"])
                    tt  = cfg["types"].get(s["type"], {})
                    e.add_field(
                        name=f"{icons.get(s['status'],'🟡')}  #{str(s['number']).zfill(4)} · {tt.get('emoji','🎫')} {tt.get('label', s['type'])}",
                        value=f"> 👤  **{mn}**\n> 📅  {s['opened_at']}\n> <#{s['channel_id']}>",
                        inline=True
                    )
                await inter.response.send_message(embed=e, view=TicketListView(g, ss, cfg), ephemeral=True)
            else:
                e.add_field(name="Aucun ticket en cours", value="> *File vide.*", inline=False)
                await inter.response.send_message(embed=e, ephemeral=True)
            return

        if val == "pending":
            gp = [(k, v) for k, v in pending_channels.items() if v["guild_id"] == g.id]
            if not gp:
                await inter.response.send_message(embed=_e_empty("📥  Salons en attente", "Aucun salon détecté."), ephemeral=True); return
            ok_cfg2, msg_cfg2 = is_configured(g.id)
            if not ok_cfg2:
                await inter.response.send_message(embed=_e_warn("⚠️", msg_cfg2), ephemeral=True); return
            if not cfg["types"]:
                await inter.response.send_message(embed=_e_warn("⚠️", "Crée d'abord des types."), ephemeral=True); return
            e = discord.Embed(description=f"### 📥  Salons en attente\n`{len(gp)}` salon(s)", color=0xE67E22)
            for k, v in gp[:10]:
                ch = g.get_channel(k)
                e.add_field(name=f"#{ch.name if ch else k}", value=f"> ⏱  {v['detected_at']}", inline=True)
            await inter.response.send_message(embed=e, view=PendingLaunchView(g, cfg, gp), ephemeral=True)
            return

        if val == "types":
            e = discord.Embed(
                description=f"### 📝  Types de tickets\n`{len(cfg['types'])}` type(s)",
                color=0x9B59B6
            )
            if cfg["types"]:
                for key, t in cfg["types"].items():
                    qs    = t.get("questions", [])
                    q_txt = "\n".join("• " + q for q in qs[:4]) or "*Aucune question*"
                    if len(qs) > 4: q_txt += f"\n*…+{len(qs)-4} autres*"
                    e.add_field(name=f"{t['emoji']}  **{t['label']}**  `[{key}]`", value=q_txt[:256], inline=False)
            else:
                e.add_field(name="Aucun type", value="> *Crée ton premier type ci-dessous.*", inline=False)
            await inter.response.send_message(embed=e, view=TypesManagerView(g, cfg), ephemeral=True)
            return

        if val == "members":
            embed = _build_embed_members(g, 0)
            await inter.response.send_message(embed=embed, view=MembersPageView(g, 0), ephemeral=True)
            return

        if val == "bots":
            embed = _build_embed_bots(g, 0)
            await inter.response.send_message(embed=embed, view=BotsPageView(g, 0), ephemeral=True)
            return

        if val == "activity":
            embed = _build_embed_activity(g, show_exempt=False, page=0)
            await inter.response.send_message(embed=embed, view=ActivityView(g, show_exempt=False, page=0), ephemeral=True)
            return

        if val == "server":
            await inter.response.send_message(embed=_build_embed_server(g), ephemeral=True)
            return

        # Sections config → édition en place du message principal
        if val in ("cfg_staff", "cfg_tickets"):
            if val == "cfg_staff":
                embed = _build_embed_config_staff(g)
                sub_view = ConfigStaffView(g, cfg)
            else:
                embed = _build_embed_config_tickets(g)
                sub_view = ConfigTicketsView(g, cfg)
            await inter.response.edit_message(embed=embed, view=DashboardView(g, val))
            await inter.followup.send(
                embed=_e_ok("⚙️  Configuration", "Utilise les menus ci-dessous pour modifier."),
                view=sub_view, ephemeral=True)
            return

        # Accueil / fallback
        await inter.response.edit_message(embed=_build_home_embed(g), view=DashboardView(g, "home"))
        active_menu_msgs[g.id] = (inter.channel.id, inter.message.id)

    async def _refresh(self, inter: discord.Interaction):
        ok2, reason = self._guard(inter)
        if not ok2:
            await inter.response.send_message(embed=_e_warn("⚠️", reason), ephemeral=True); return
        await inter.response.edit_message(embed=_build_home_embed(self.guild), view=DashboardView(self.guild, "home"))
        active_menu_msgs[self.guild.id] = (inter.channel.id, inter.message.id)

    async def _close(self, inter: discord.Interaction):
        try: await inter.message.delete()
        except Exception: pass
        active_menu_msgs.pop(self.guild.id, None)
        await inter.response.send_message(embed=_e_empty("✖️  Panel fermé", "Rouvre avec `/menu`."), ephemeral=True)
        self.stop()


# ══════════════════════════════════════════════════════════════════
#  ActivityView — sous-vue éphémère pour explorer l'activité
# ══════════════════════════════════════════════════════════════════

class ExemptRolesView(discord.ui.View):
    """View paginée pour sélectionner les rôles exemptés d'analyse publique."""
    def __init__(self, guild: discord.Guild, cfg: dict, page: int = 0):
        super().__init__(timeout=300)
        self.guild = guild
        self.cfg   = cfg
        self.page  = page
        all_roles  = [r for r in guild.roles if r.name != "@everyone"]
        all_roles.sort(key=lambda r: r.position, reverse=True)
        exempt_ids = cfg.get("activity_exempt_role_ids", [])
        _, total_pages, _, _ = _paginate(all_roles, page, SEL_ITEMS)
        self._total = total_pages
        start = page * SEL_ITEMS
        chunk = all_roles[start:start + SEL_ITEMS]
        if chunk:
            opts = [discord.SelectOption(
                label=r.name[:50],
                description=f"{len(r.members)} membre(s){'  ✅ exempté' if r.id in exempt_ids else ''}",
                value=str(r.id),
                default=(r.id in exempt_ids)
            ) for r in chunk]
            sel = discord.ui.Select(
                placeholder=f"🙈  Sélectionner les rôles exemptés (p.{page+1}/{total_pages})…",
                options=opts, min_values=0, max_values=len(opts), row=0
            )
            sel.callback = self._save
            self.add_item(sel)
        btn_prev = discord.ui.Button(label="◀  Précédent", style=discord.ButtonStyle.secondary,
                                     disabled=(page == 0), row=1)
        btn_prev.callback = self._prev
        self.add_item(btn_prev)
        btn_next = discord.ui.Button(label="Suivant  ▶", style=discord.ButtonStyle.secondary,
                                     disabled=(page >= total_pages - 1), row=1)
        btn_next.callback = self._next
        self.add_item(btn_next)

    async def _save(self, inter: discord.Interaction):
        # Ajouter/retirer les rôles sélectionnés sur cette page
        # Garder les exemptions des autres pages intactes
        all_roles = [r for r in self.guild.roles if r.name != "@everyone"]
        all_roles.sort(key=lambda r: r.position, reverse=True)
        start = self.page * SEL_ITEMS
        page_role_ids = {r.id for r in all_roles[start:start + SEL_ITEMS]}
        # IDs exemptés sur les autres pages (à conserver)
        current = set(self.cfg.get("activity_exempt_role_ids", []))
        other_pages = current - page_role_ids
        # IDs sélectionnés sur cette page
        selected = {int(v) for v in inter.data["values"]}
        self.cfg["activity_exempt_role_ids"] = list(other_pages | selected)
        await save_data()
        names = [self.guild.get_role(rid).name for rid in selected if self.guild.get_role(rid)]
        await inter.response.send_message(
            embed=_e_ok("✅  Rôles exemptés mis à jour",
                         f"{len(selected)} rôle(s) exempté(s) sur cette page : "
                         + (", ".join(names) if names else "aucun")),
            ephemeral=True)

    async def _prev(self, inter: discord.Interaction):
        await inter.response.edit_message(
            embed=discord.Embed(title="🙈  Rôles exemptés",
                description="Sélectionne les rôles dont les membres seront masqués publiquement.",
                color=0x5865F2),
            view=ExemptRolesView(self.guild, self.cfg, self.page - 1))

    async def _next(self, inter: discord.Interaction):
        await inter.response.edit_message(
            embed=discord.Embed(title="🙈  Rôles exemptés",
                description="Sélectionne les rôles dont les membres seront masqués publiquement.",
                color=0x5865F2),
            view=ExemptRolesView(self.guild, self.cfg, self.page + 1))


class MemberActivityRefreshView(discord.ui.View):
    """View avec bouton Actualiser pour le rapport d'activité individuel."""
    def __init__(self, guild: discord.Guild, member_id: int):
        super().__init__(timeout=300)
        self.guild     = guild
        self.member_id = member_id
        btn = discord.ui.Button(
            label="Actualiser",
            style=discord.ButtonStyle.secondary,
            emoji="🔄",
            row=0
        )
        btn.callback = self._refresh
        self.add_item(btn)

    async def _refresh(self, inter: discord.Interaction):
        m = self.guild.get_member(self.member_id)
        if not m:
            await inter.response.send_message(embed=_e_err("❌", "Membre introuvable."), ephemeral=True); return
        embed = _member_activity_embed(m, self.guild.id)
        view  = MemberActivityRefreshView(self.guild, self.member_id)
        await inter.response.edit_message(embed=embed, view=view)

class ActivityView(discord.ui.View):
    """View paginée pour l'analyse d'activité — boutons ◀ ▶ + select rapport individuel."""
    def __init__(self, guild: discord.Guild, show_exempt: bool = False, page: int = 0):
        super().__init__(timeout=300)
        self.guild        = guild
        self.show_exempt  = show_exempt
        self.page         = page

        # Construire la liste triée
        humans = [m for m in guild.members if not m.bot]
        rows = [(m, _inactivity_score(guild.id, m.id), _is_exempt(m, guild.id))
                for m in humans
                if show_exempt or not _is_exempt(m, guild.id)]
        rows.sort(key=lambda x: x[1], reverse=True)
        self._rows       = rows
        self._total_pages = max(1, (len(rows) + SEL_ITEMS - 1) // SEL_ITEMS)
        page              = max(0, min(page, self._total_pages - 1))
        self.page         = page

        # Select — membres de la page courante
        start = page * SEL_ITEMS
        chunk = rows[start:start + SEL_ITEMS]
        if chunk:
            opts = []
            for m, score, exempt in chunk:
                emoji, label = _score_label(score)
                tag = " 🙈" if exempt else ""
                opts.append(discord.SelectOption(
                    label=f"{m.display_name[:38]}{tag}",
                    description=f"{emoji} Score : {score}/100 — {label}",
                    value=str(m.id)
                ))
            sel = discord.ui.Select(
                placeholder=f"🔍  Rapport individuel… (p.{page+1}/{self._total_pages})",
                options=opts, row=0
            )
            sel.callback = self._on_select
            self.add_item(sel)

        # Boutons navigation
        btn_prev = discord.ui.Button(
            label="◀  Précédent", style=discord.ButtonStyle.secondary,
            disabled=(page == 0), row=1
        )
        btn_prev.callback = self._prev
        self.add_item(btn_prev)

        btn_next = discord.ui.Button(
            label="Suivant  ▶", style=discord.ButtonStyle.secondary,
            disabled=(page >= self._total_pages - 1), row=1
        )
        btn_next.callback = self._next
        self.add_item(btn_next)

        btn_toggle = discord.ui.Button(
            label="Voir exemptés" if not show_exempt else "Masquer exemptés",
            style=discord.ButtonStyle.primary, emoji="🙈", row=1
        )
        btn_toggle.callback = self._toggle_exempt
        self.add_item(btn_toggle)

    async def _on_select(self, inter: discord.Interaction):
        mid = int(inter.data["values"][0])
        m   = self.guild.get_member(mid)
        if not m:
            await inter.response.send_message(embed=_e_err("❌", "Membre introuvable."), ephemeral=True); return
        embed = _member_activity_embed(m, self.guild.id)
        view  = MemberActivityRefreshView(self.guild, m.id)
        await inter.response.send_message(embed=embed, view=view, ephemeral=True)

    async def _prev(self, inter: discord.Interaction):
        new_view = ActivityView(self.guild, self.show_exempt, self.page - 1)
        embed    = _build_embed_activity(self.guild, self.show_exempt, self.page - 1)
        await inter.response.edit_message(embed=embed, view=new_view)

    async def _next(self, inter: discord.Interaction):
        new_view = ActivityView(self.guild, self.show_exempt, self.page + 1)
        embed    = _build_embed_activity(self.guild, self.show_exempt, self.page + 1)
        await inter.response.edit_message(embed=embed, view=new_view)

    async def _toggle_exempt(self, inter: discord.Interaction):
        new_exempt = not self.show_exempt
        new_view   = ActivityView(self.guild, new_exempt, 0)
        embed      = _build_embed_activity(self.guild, new_exempt, 0)
        try:
            await inter.response.edit_message(embed=embed, view=new_view)
        except Exception:
            # Si le message éphémère est expiré, envoyer un nouveau
            await inter.response.send_message(embed=embed, view=new_view, ephemeral=True)


# ══════════════════════════════════════════════════════════════════
#  ConfigStaffView — config staff (éphémère)
# ══════════════════════════════════════════════════════════════════
class ConfigStaffView(discord.ui.View):
    def __init__(self, guild: discord.Guild, cfg: dict, role_page: int = 0):
        super().__init__(timeout=300)
        self.guild     = guild
        self.cfg       = cfg
        self.role_page = role_page

        chs       = sorted([c for c in guild.channels if isinstance(c, discord.TextChannel)], key=lambda c: c.name)
        all_roles = [r for r in guild.roles if r.name != "@everyone"]

        if chs:
            s_ch = discord.ui.Select(placeholder="📣  Salon notifications staff…", options=[
                discord.SelectOption(label=f"#{c.name}"[:50], value=str(c.id),
                    default=(cfg.get("staff_channel_id") == c.id)) for c in chs[:25]], row=0)
            s_ch.callback = self._set_channel
            self.add_item(s_ch)

        if all_roles:
            page_start = role_page * 23
            page_roles = all_roles[page_start:page_start + 23]
            has_next   = (page_start + 23) < len(all_roles)
            opts = [discord.SelectOption(
                label=r.name[:50], description=f"{len(r.members)} membre(s)", value=str(r.id),
                default=(cfg.get("staff_role_id") == r.id)) for r in page_roles]
            if has_next:
                opts.append(discord.SelectOption(label=f"➡️  Suite ({len(all_roles)-page_start-23} rôles)",
                    value="__next__", description="Voir la page suivante"))
            s_role = discord.ui.Select(
                placeholder=f"🛡️  Rôle staff{' (p.'+str(role_page+1)+')' if len(all_roles)>23 else ''}…",
                options=opts, row=1)
            s_role.callback = self._set_role
            self.add_item(s_role)

        # Bouton toggle anon (dynamique selon état actuel)
        anon_on  = cfg.get("anon_enabled", True)
        btn_anon = discord.ui.Button(
            label="Désactiver messages anonymes" if anon_on else "Activer messages anonymes",
            style=discord.ButtonStyle.danger if anon_on else discord.ButtonStyle.success,
            emoji="🔇" if anon_on else "💬", row=2)
        btn_anon.callback = self._toggle_anon
        self.add_item(btn_anon)

        btn_exempt = discord.ui.Button(label="Rôles exemptés activité", style=discord.ButtonStyle.secondary,
                                       emoji="🙈", row=2)
        btn_exempt.callback = self._set_exempt
        self.add_item(btn_exempt)

        btn_search = discord.ui.Button(label="Chercher un rôle", style=discord.ButtonStyle.secondary,
                                       emoji="🔍", row=3)
        btn_search.callback = self._search_role
        self.add_item(btn_search)

    def _guard(self, inter):
        if not is_staff(inter.user, self.guild.id): return False, "🚫"
        return True, ""

    async def _set_channel(self, inter: discord.Interaction):
        ok2, r = self._guard(inter)
        if not ok2: await inter.response.send_message(embed=_e_err("🚫", r), ephemeral=True); return
        self.cfg["staff_channel_id"] = int(inter.data["values"][0])
        await save_data()
        ch = self.guild.get_channel(self.cfg["staff_channel_id"])
        await inter.response.send_message(embed=_e_ok("✅  Salon mis à jour", f"**#{ch.name if ch else '?'}**"), ephemeral=True)

    async def _set_role(self, inter: discord.Interaction):
        ok2, r = self._guard(inter)
        if not ok2: await inter.response.send_message(embed=_e_err("🚫", r), ephemeral=True); return
        val = inter.data["values"][0]
        if val == "__next__":
            await inter.response.edit_message(view=ConfigStaffView(self.guild, self.cfg, self.role_page+1)); return
        self.cfg["staff_role_id"] = int(val)
        await save_data()
        role = self.guild.get_role(self.cfg["staff_role_id"])
        await inter.response.send_message(embed=_e_ok("✅  Rôle staff mis à jour", f"**{role.name if role else '?'}**"), ephemeral=True)

    async def _toggle_anon(self, inter: discord.Interaction):
        ok2, r = self._guard(inter)
        if not ok2: await inter.response.send_message(embed=_e_err("🚫", r), ephemeral=True); return
        self.cfg["anon_enabled"] = not self.cfg.get("anon_enabled", True)
        await save_data()
        state = self.cfg["anon_enabled"]
        await inter.response.edit_message(view=ConfigStaffView(self.guild, self.cfg, self.role_page))
        await inter.followup.send(
            embed=_e_ok("✅  Messages anonymes " + ("activés" if state else "désactivés"),
                         "Les membres " + ("peuvent" if state else "ne peuvent plus") + " utiliser `/anon`."),
            ephemeral=True)

    async def _set_exempt(self, inter: discord.Interaction):
        ok2, r = self._guard(inter)
        if not ok2: await inter.response.send_message(embed=_e_err("🚫", r), ephemeral=True); return
        total_roles = len([r for r in self.guild.roles if r.name != "@everyone"])
        await inter.response.send_message(
            embed=discord.Embed(
                title="🙈  Rôles exemptés d'analyse publique",
                description=(
                    f"Sélectionne les rôles dont les membres ne seront visibles **que sur la console**.\n"
                    f"`{total_roles}` rôles disponibles — navigation par pages de {SEL_ITEMS}"
                ),
                color=0x5865F2),
            view=ExemptRolesView(self.guild, self.cfg, 0),
            ephemeral=True)

    async def _search_role(self, inter: discord.Interaction):
        ok2, r = self._guard(inter)
        if not ok2: await inter.response.send_message(embed=_e_err("🚫", r), ephemeral=True); return
        await inter.response.send_modal(RoleSearchModal(self.guild, self.cfg))


# ══════════════════════════════════════════════════════════════════
#  ConfigTicketsView — config tickets (éphémère)
# ══════════════════════════════════════════════════════════════════
class ConfigTicketsView(discord.ui.View):
    def __init__(self, guild: discord.Guild, cfg: dict):
        super().__init__(timeout=300)
        self.guild = guild
        self.cfg   = cfg
        cats = sorted(guild.categories, key=lambda c: c.position)
        if cats:
            s_cat = discord.ui.Select(placeholder="📁  Catégorie des tickets…", options=[
                discord.SelectOption(label=cat.name[:50], description=f"{len(cat.channels)} salon(s)",
                    value=str(cat.id), default=(cfg.get("category_id") == cat.id))
                for cat in cats[:25]], row=0)
            s_cat.callback = self._set_cat
            self.add_item(s_cat)
        btn_types = discord.ui.Button(label="Gérer les types", style=discord.ButtonStyle.primary, emoji="📝", row=1)
        btn_types.callback = self._open_types
        self.add_item(btn_types)

    def _guard(self, inter):
        if not is_staff(inter.user, self.guild.id): return False, "🚫"
        return True, ""

    async def _set_cat(self, inter: discord.Interaction):
        ok2, r = self._guard(inter)
        if not ok2: await inter.response.send_message(embed=_e_err("🚫", r), ephemeral=True); return
        self.cfg["category_id"] = int(inter.data["values"][0])
        await save_data()
        cat = self.guild.get_channel(self.cfg["category_id"])
        await inter.response.send_message(embed=_e_ok("✅  Catégorie mise à jour", f"**{cat.name if cat else '?'}**"), ephemeral=True)

    async def _open_types(self, inter: discord.Interaction):
        ok2, r = self._guard(inter)
        if not ok2: await inter.response.send_message(embed=_e_err("🚫", r), ephemeral=True); return
        cfg = tconf(self.guild.id)
        e = discord.Embed(description=f"### 📝  Types de tickets\n`{len(cfg['types'])}` type(s)", color=0x9B59B6)
        if cfg["types"]:
            for key, t in cfg["types"].items():
                qs    = t.get("questions", [])
                q_txt = "\n".join("• " + q for q in qs[:4]) or "*Aucune question*"
                if len(qs) > 4: q_txt += f"\n*…+{len(qs)-4} autres*"
                e.add_field(name=f"{t['emoji']}  **{t['label']}**  `[{key}]`", value=q_txt[:256], inline=False)
        else:
            e.add_field(name="Aucun type", value="> *Crée ton premier type ci-dessous.*", inline=False)
        await inter.response.send_message(embed=e, view=TypesManagerView(self.guild, cfg), ephemeral=True)


class TicketListView(discord.ui.View):
    def __init__(self, guild, sessions, cfg):
        super().__init__(timeout=120)
        self.guild = guild
        self.cfg   = cfg
        icons = {"open": "🟡", "pending": "⏳"}
        opts  = []
        for s in sorted(sessions, key=lambda x: x["number"])[:25]:
            mem = guild.get_member(s["user_id"])
            mn  = (mem.display_name if mem else str(s["user_id"]))[:20]
            ico = icons.get(s["status"], "🟡")
            opts.append(discord.SelectOption(
                label=(ico + "  #" + str(s["number"]).zfill(4) + " · " + s["type"])[:100],
                description=("Par " + mn + " · " + s["opened_at"])[:100],
                value=str(s["channel_id"])))
        sel = discord.ui.Select(placeholder="🎫  Choisir un ticket…", options=opts)
        sel.callback = self.on_select
        self.add_item(sel)

    async def on_select(self, inter: discord.Interaction):
        s = ticket_sessions.get(int(inter.data["values"][0]))
        if not s:
            await inter.response.send_message(embed=_e_err("❌  Ticket introuvable"), ephemeral=True)
            return
        mem  = self.guild.get_member(s["user_id"])
        mn   = mem.display_name if mem else str(s["user_id"])
        tt   = self.cfg["types"].get(s["type"], {})
        cols = {"open": C_ORANGE, "pending": C_AMBER, "accepted": C_GREEN, "refused": C_RED, "closed": C_GREY}
        stat_lbl = {"open": "🟡 Ouvert", "pending": "⏳ En attente",
                    "accepted": "✅ Accepté", "refused": "❌ Refusé", "closed": "🔒 Fermé"}
        e = discord.Embed(
            title=tt.get("emoji", "🎫") + "  Ticket #" + str(s["number"]).zfill(4) + " — " + tt.get("label", s["type"]),
            color=cols.get(s["status"], 0x99AAB5))
        e.add_field(name="👤  Demandeur", value=mem.mention if mem else mn,         inline=True)
        e.add_field(name="📊  Statut",   value=stat_lbl.get(s["status"], s["status"]), inline=True)
        e.add_field(name="📅  Ouvert le",value=s["opened_at"],                      inline=True)
        e.add_field(name="📍  Salon",    value="<#" + str(s["channel_id"]) + ">",   inline=True)
        for q, a in list(s["answers"].items())[:4]:
            e.add_field(name="❓  " + q[:40], value="```" + a[:100] + "```", inline=False)
        await inter.response.send_message(embed=e, view=TicketActionView(self.guild, s), ephemeral=True)


# ── Vue : actions sur ticket ─────────────────────────────────────
class TicketActionView(discord.ui.View):
    """Vue persistante : buttons always-on tant que le ticket est pending.
    timeout=None + custom_id fixes = survie aux redémarrages bot.
    """
    def __init__(self, guild, s):
        super().__init__(timeout=None)  # jamais d'expiration
        self.guild = guild
        self.s     = s

    def _resolve_session(self, inter: discord.Interaction):
        """
        Retrouve la session ticket depuis l'interaction.
        Priorité : staff_msg_id == message cliqué → channel_id de self.s
        """
        # 1. Lookup par message ID (le plus fiable)
        s = next(
            (x for x in ticket_sessions.values()
             if x.get("staff_msg_id") == inter.message.id),
            None
        )
        # 2. Fallback : retrouver via channel_id stocké dans self.s
        if not s and self.s:
            ch_id = self.s.get("channel_id")
            s = ticket_sessions.get(ch_id) if ch_id else None
        # 3. Fallback final : self.s lui-même
        if not s:
            s = self.s
        if not s:
            asyncio.create_task(
                inter.response.send_message(
                    embed=_e_err("❌  Session introuvable",
                                "Ce ticket n'existe plus en mémoire."), ephemeral=True))
            return None
        # Vérifier que le ticket est encore actionnable
        if s.get("status") not in ("open", "pending", None):
            asyncio.create_task(self._disable_silently(inter))
            return None
        self.s = s  # mettre à jour la référence locale
        return s

    async def _disable_silently(self, inter: discord.Interaction):
        """Désactive les boutons si le ticket est déjà traité."""
        for item in self.children:
            item.disabled = True
        try:
            if not inter.response.is_done():
                await inter.response.edit_message(view=self)
                await inter.followup.send(
                    embed=_e_warn("⚠️  Déjà traité",
                                  "Ce ticket a déjà été traité. Les boutons ont été désactivés."),
                    ephemeral=True)
            else:
                await inter.message.edit(view=self)
        except: pass
        self.stop()

    def _guard(self, inter):
        if not is_staff(inter.user, self.guild.id):
            if not inter.response.is_done():
                asyncio.create_task(
                    inter.response.send_message(
                        embed=_e_err("🚫  Accès refusé", "Réservé au staff."), ephemeral=True))
            return False
        return True

    async def _lock_buttons(self, inter: discord.Interaction):
        """Désactive tous les boutons et met à jour le message original."""
        for item in self.children:
            item.disabled = True
        try:
            await inter.message.edit(view=self)
        except: pass
        self.stop()

    @discord.ui.button(label="Accepter", style=discord.ButtonStyle.success,
                        emoji="✅", row=0, custom_id="tkt_accept")
    async def accept(self, inter: discord.Interaction, btn: discord.ui.Button):
        s = self._resolve_session(inter)
        if not s: return
        if not self._guard(inter):
            await inter.response.send_message(embed=_e_err("🚫", "Réservé au staff."), ephemeral=True); return
        await inter.response.send_modal(
            ActionCommentModal(self.guild, s, "accepted", inter.message))

    @discord.ui.button(label="Refuser", style=discord.ButtonStyle.danger,
                        emoji="❌", row=0, custom_id="tkt_refuse")
    async def refuse(self, inter: discord.Interaction, btn: discord.ui.Button):
        s = self._resolve_session(inter)
        if not s: return
        if not self._guard(inter):
            await inter.response.send_message(embed=_e_err("🚫", "Réservé au staff."), ephemeral=True); return
        await inter.response.send_modal(
            ActionCommentModal(self.guild, s, "refused", inter.message))

    @discord.ui.button(label="Fermer", style=discord.ButtonStyle.secondary,
                        emoji="🔒", row=0, custom_id="tkt_close")
    async def close(self, inter: discord.Interaction, btn: discord.ui.Button):
        s = self._resolve_session(inter)
        if not s: return
        if not self._guard(inter):
            await inter.response.send_message(embed=_e_err("🚫", "Réservé au staff."), ephemeral=True); return
        for item in self.children: item.disabled = True
        await inter.response.edit_message(view=self)
        self.stop()
        await do_action(self.guild, s, "closed", "", inter.user)
        await inter.followup.send(embed=_e_empty("🔒  Fermé", "Salon supprimé sous peu."), ephemeral=True)

    @discord.ui.button(label="Message", style=discord.ButtonStyle.primary,
                        emoji="💬", row=0, custom_id="tkt_msg")
    async def send_msg(self, inter: discord.Interaction, btn: discord.ui.Button):
        s = self._resolve_session(inter)
        if not s: return
        if not self._guard(inter):
            await inter.response.send_message(embed=_e_err("🚫", "Réservé au staff."), ephemeral=True); return
        await inter.response.send_modal(SendMsgModal(self.guild, s))


class ActionCommentModal(discord.ui.Modal):
    """Modal affiché lors d'un Accepter ou Refuser — permet de saisir un commentaire."""
    commentaire = discord.ui.TextInput(
        label="Commentaire (optionnel)",
        placeholder="Laisse vide pour ne rien ajouter…",
        style=discord.TextStyle.paragraph,
        max_length=500,
        required=False)

    def __init__(self, guild, s, action, origin_msg=None):
        label = "✅  Accepter le ticket" if action == "accepted" else "❌  Refuser le ticket"
        super().__init__(title=label)
        self.guild      = guild
        self.s          = s
        self.action     = action
        self.origin_msg = origin_msg  # message Discord contenant les boutons

    async def on_submit(self, inter: discord.Interaction):
        commentaire = self.commentaire.value.strip()
        await do_action(self.guild, self.s, self.action, commentaire, inter.user)
        # Désactiver les boutons du message d'origine
        if self.origin_msg:
            try:
                # Passer view=None supprime complètement les boutons
                # tout en conservant l'embed visible
                await self.origin_msg.edit(view=None)
            except: pass
        if self.action == "accepted":
            desc = ("💬 " + commentaire) if commentaire else "Aucun commentaire."
            await inter.response.send_message(embed=_e_ok("✅  Ticket accepté", desc), view=PostActionView(self.guild, self.s), ephemeral=True)
        else:
            desc = ("💬 " + commentaire) if commentaire else "Aucun commentaire."
            await inter.response.send_message(embed=_e_err("❌  Ticket refusé", desc), view=PostActionView(self.guild, self.s), ephemeral=True)


class PostActionView(discord.ui.View):
    """Vue après accept/refuse : Fermer le salon + Envoyer un message."""
    def __init__(self, guild, s):
        super().__init__(timeout=300)
        self.guild = guild
        self.s     = s

    @discord.ui.button(label="Fermer le salon", style=discord.ButtonStyle.secondary, emoji="🔒")
    async def close(self, inter: discord.Interaction, btn: discord.ui.Button):
        if not is_staff(inter.user, self.guild.id):
            await inter.response.send_message(embed=_e_err("🚫"), ephemeral=True); return
        await do_action(self.guild, self.s, "closed", "", inter.user)
        await inter.response.send_message(embed=_e_empty("🔒  Fermé", "Salon supprimé sous peu."), ephemeral=True)
        self.stop()

    @discord.ui.button(label="Envoyer message", style=discord.ButtonStyle.primary, emoji="💬", row=0)
    async def send_msg(self, inter: discord.Interaction, btn: discord.ui.Button):
        if not is_staff(inter.user, self.guild.id):
            await inter.response.send_message(embed=_e_err("🚫"), ephemeral=True); return
        await inter.response.send_modal(SendMsgModal(self.guild, self.s))

    @discord.ui.button(label="Actions enrichies", style=discord.ButtonStyle.success, emoji="⚡", row=1)
    async def extra_actions(self, inter: discord.Interaction, btn: discord.ui.Button):
        if not is_staff(inter.user, self.guild.id):
            await inter.response.send_message(embed=_e_err("🚫"), ephemeral=True); return
        await inter.response.send_message(
            embed=discord.Embed(title="⚡  Actions enrichies", color=0x1ABC9C,
                description="Que veux-tu faire pour ce membre ?"),
            view=ExtraActionsView(self.guild, self.s), ephemeral=True)


# ── Vue : actions enrichies ───────────────────────────────────
class ExtraActionsView(discord.ui.View):
    def __init__(self, guild, s):
        super().__init__(timeout=120)
        self.guild = guild; self.s = s

    @discord.ui.button(label="Créer invitation (1 usage)", style=discord.ButtonStyle.primary, emoji="🔗", row=0)
    async def create_invite(self, inter: discord.Interaction, btn: discord.ui.Button):
        if not is_staff(inter.user, self.guild.id):
            await inter.response.send_message(embed=_e_err("🚫"), ephemeral=True); return
        errs = await do_accept_actions(self.guild, self.s, {"invite": True})
        if errs: await inter.response.send_message(embed=_e_warn("⚠️", "\n".join(errs)), ephemeral=True)
        else:    await inter.response.send_message(embed=_e_ok("🔗  Invitation envoyée dans le salon ticket"), ephemeral=True)

    @discord.ui.button(label="Attribuer un rôle", style=discord.ButtonStyle.secondary, emoji="🎭", row=0)
    async def add_role(self, inter: discord.Interaction, btn: discord.ui.Button):
        if not is_staff(inter.user, self.guild.id):
            await inter.response.send_message(embed=_e_err("🚫"), ephemeral=True); return
        roles = [r for r in self.guild.roles if r.name != "@everyone" and r < self.guild.me.top_role]
        if not roles:
            await inter.response.send_message(embed=_e_warn("⚠️", "Aucun rôle attribuable."), ephemeral=True); return
        opts = [discord.SelectOption(label=r.name[:50], value=str(r.id)) for r in roles[:25]]
        sel  = discord.ui.Select(placeholder="Choisir le rôle…", options=opts)
        s_ref = self.s; guild_ref = self.guild
        async def _pick(i2):
            rid = int(i2.data["values"][0])
            errs = await do_accept_actions(guild_ref, s_ref, {"add_role": rid})
            r = guild_ref.get_role(rid)
            if errs: await i2.response.send_message(embed=_e_warn("⚠️", "\n".join(errs)), ephemeral=True)
            else:    await i2.response.send_message(embed=_e_ok("🎭  Rôle attribué", "**" + (r.name if r else "?") + "**"), ephemeral=True)
        sel.callback = _pick
        v2 = discord.ui.View(timeout=60); v2.add_item(sel)
        await inter.response.send_message(embed=discord.Embed(title="🎭  Quel rôle ?", color=0x9B59B6), view=v2, ephemeral=True)

    @discord.ui.button(label="Renommer le membre", style=discord.ButtonStyle.secondary, emoji="✏️", row=0)
    async def rename_member(self, inter: discord.Interaction, btn: discord.ui.Button):
        if not is_staff(inter.user, self.guild.id):
            await inter.response.send_message(embed=_e_err("🚫"), ephemeral=True); return
        await inter.response.send_modal(RenameModal(self.guild, self.s))

    @discord.ui.button(label="Créer un rôle", style=discord.ButtonStyle.success, emoji="➕", row=1)
    async def create_role_btn(self, inter: discord.Interaction, btn: discord.ui.Button):
        if not is_staff(inter.user, self.guild.id):
            await inter.response.send_message(embed=_e_err("🚫"), ephemeral=True); return
        await inter.response.send_modal(CreateRoleModal(self.guild, self.s))


class RenameModal(discord.ui.Modal, title="✏️  Renommer le membre"):
    new_nick = discord.ui.TextInput(label="Nouveau pseudo (max 32 car.)",
        placeholder="Laisse vide pour réinitialiser…", max_length=32, required=False)
    def __init__(self, guild, s):
        super().__init__(); self.guild = guild; self.s = s
    async def on_submit(self, inter: discord.Interaction):
        errs = await do_accept_actions(self.guild, self.s, {"rename": self.new_nick.value or None})
        if errs: await inter.response.send_message(embed=_e_warn("⚠️", "\n".join(errs)), ephemeral=True)
        else:    await inter.response.send_message(embed=_e_ok("✏️  Pseudo modifié", self.new_nick.value or "(réinitialisé)"), ephemeral=True)


class CreateRoleModal(discord.ui.Modal, title="➕  Créer un rôle"):
    role_name = discord.ui.TextInput(label="Nom du rôle", placeholder="ex: VIP, Accès serveur…", max_length=50)
    def __init__(self, guild, s):
        super().__init__(); self.guild = guild; self.s = s
    async def on_submit(self, inter: discord.Interaction):
        errs = await do_accept_actions(self.guild, self.s, {"create_role": self.role_name.value.strip()})
        if errs: await inter.response.send_message(embed=_e_warn("⚠️", "\n".join(errs)), ephemeral=True)
        else:    await inter.response.send_message(embed=_e_ok("➕  Rôle créé et attribué", "**" + self.role_name.value.strip() + "**"), ephemeral=True)


class SendMsgModal(discord.ui.Modal, title="💬  Message dans le ticket"):
    message = discord.ui.TextInput(label="Message", placeholder="Votre message pour le demandeur…",
                                    style=discord.TextStyle.paragraph, max_length=1000)

    def __init__(self, guild, s):
        super().__init__()
        self.guild = guild
        self.s     = s

    async def on_submit(self, inter: discord.Interaction):
        ch = self.guild.get_channel(self.s["channel_id"])
        if not ch:
            await inter.response.send_message(embed=_e_err("❌  Salon introuvable"), ephemeral=True); return
        await ch.send("📢  **Staff** (" + inter.user.display_name + ") :\n" + self.message.value)
        await inter.response.send_message(embed=_e_ok("✅  Message envoyé"), ephemeral=True)


# ── Vue : salons en attente ──────────────────────────────────────
class PendingLaunchView(discord.ui.View):
    def __init__(self, guild, cfg, pending_list):
        super().__init__(timeout=120)
        self.guild = guild
        self.cfg   = cfg
        self.sel_ch = None
        opts = []
        for k, v in pending_list[:25]:
            ch = guild.get_channel(k)
            opts.append(discord.SelectOption(
                label="#" + (ch.name if ch else str(k)),
                description="Détecté le " + v["detected_at"],
                value=str(k), emoji="📥"))
        s1 = discord.ui.Select(placeholder="1️⃣  Choisir le salon…", options=opts)
        s1.callback = self.sel_channel
        self.add_item(s1)

    async def sel_channel(self, inter: discord.Interaction):
        self.sel_ch = int(inter.data["values"][0])
        opts_t = []
        for k, v in self.cfg["types"].items():
            opts_t.append(discord.SelectOption(
                label=v["emoji"] + "  " + v["label"],
                description="Clé : " + k + "  ·  " + str(len(v.get("questions", []))) + " question(s)",
                value=k))
        s2 = discord.ui.Select(placeholder="2️⃣  Type de questions…", options=opts_t)
        s2.callback = self.sel_type
        v2 = discord.ui.View(timeout=60)
        v2.add_item(s2)
        e = discord.Embed(title="2️⃣  Choisir le type", description="Quel type de ticket est-ce ?", color=C_AMBER)
        await inter.response.send_message(embed=e, view=v2, ephemeral=True)

    async def sel_type(self, inter: discord.Interaction):
        key = inter.data["values"][0]
        ch  = self.guild.get_channel(self.sel_ch)
        if not ch:
            await inter.response.send_message(embed=_e_err("❌  Salon introuvable"), ephemeral=True); return

        # Priorité au propriétaire détecté
        owner_id = pending_channels.get(self.sel_ch, {}).get("owner_id")
        owner    = self.guild.get_member(owner_id) if owner_id else None

        membres = [m for m in self.guild.members if not m.bot and ch.permissions_for(m).view_channel
                   and not m.guild_permissions.administrator]
        if not membres:
            membres = [m for m in self.guild.members if not m.bot and ch.permissions_for(m).view_channel]
        if not membres:
            await inter.response.send_message(embed=_e_err("❌  Aucun membre dans ce salon"), ephemeral=True); return

        # Mettre le owner en tête de liste
        if owner and owner in membres:
            membres = [owner] + [m for m in membres if m.id != owner.id]
        elif owner:
            membres = [owner] + membres

        self._sel_key = key
        self._sel_ch  = self.sel_ch
        opts = [discord.SelectOption(
            label=("⭐ " if m == owner else "") + m.display_name[:47],
            description=("Propriétaire détecté — " if m == owner else "ID: ") + str(m.id),
            value=str(m.id),
            default=(m == owner)) for m in membres[:25]]
        sel_m = discord.ui.Select(placeholder="3️⃣  Choisir le demandeur…", options=opts)
        sel_m.callback = self.sel_member
        v3 = discord.ui.View(timeout=60)
        v3.add_item(sel_m)
        tt = self.cfg["types"][key]
        e  = discord.Embed(title="3️⃣  Qui est le demandeur ?",
                           description="Type : **" + tt["emoji"] + " " + tt["label"] + "**\n" +
                                       ("⭐ Propriétaire détecté : **" + owner.display_name + "**" if owner else "*Aucun propriétaire détecté*"),
                           color=C_ORANGE)
        await inter.response.send_message(embed=e, view=v3, ephemeral=True)

    async def sel_member(self, inter: discord.Interaction):
        member = self.guild.get_member(int(inter.data["values"][0]))
        ch     = self.guild.get_channel(self.sel_ch)
        key    = self._sel_key
        if not member or not ch:
            await inter.response.send_message(embed=_e_err("❌  Introuvable"), ephemeral=True); return
        tt = self.cfg["types"][key]
        e  = discord.Embed(title="▶️  Questions lancées",
                           description="**" + tt["emoji"] + " " + tt["label"] + "** dans " + ch.mention + " pour " + member.mention + ".",
                           color=C_GREEN)
        await inter.response.send_message(embed=e, ephemeral=True)
        asyncio.create_task(run_questions(self.guild, self.cfg, ch, member, key))


# ── Vue : configuration complète ─────────────────────────────────
class RoleSearchModal(discord.ui.Modal, title="🔍  Chercher un rôle par nom"):
    query = discord.ui.TextInput(label="Nom du rôle (partiel accepté)",
                                  placeholder="ex: Admin, Modérateur…",
                                  max_length=50)

    def __init__(self, guild, cfg):
        super().__init__()
        self.guild = guild
        self.cfg   = cfg

    async def on_submit(self, inter: discord.Interaction):
        q = self.query.value.lower()
        matches = [r for r in self.guild.roles if q in r.name.lower() and r.name != "@everyone"]
        if not matches:
            await inter.response.send_message(embed=_e_err("❌  Aucun rôle trouvé", "Essaie avec un autre nom."), ephemeral=True); return
        if len(matches) == 1:
            self.cfg["staff_role_id"] = matches[0].id
            await save_data()
            await inter.response.send_message(embed=_e_ok("✅  Rôle staff mis à jour", "**" + matches[0].name + "**"), ephemeral=True)
            return
        # Plusieurs correspondances → proposer un select
        opts = [discord.SelectOption(label=r.name[:50], description=str(len(r.members)) + " membre(s)", value=str(r.id))
                for r in matches[:25]]
        sel = discord.ui.Select(placeholder="Choisir parmi les résultats…", options=opts)
        cfg_ref = self.cfg
        guild_ref = self.guild
        async def _pick(i2):
            cfg_ref["staff_role_id"] = int(i2.data["values"][0])
            await save_data()
            r2 = guild_ref.get_role(cfg_ref["staff_role_id"])
            await i2.response.send_message(embed=_e_ok("✅  Rôle staff mis à jour", "**" + (r2.name if r2 else "?") + "**"), ephemeral=True)
        sel.callback = _pick
        v2 = discord.ui.View(timeout=60); v2.add_item(sel)
        await inter.response.send_message(embed=discord.Embed(title="🔍  Résultats — " + str(len(matches)) + " rôle(s)", color=C_BLUE),
                                          view=v2, ephemeral=True)


# ── Vue : gestion complète des types depuis Discord ───────────────
class TypesManagerView(discord.ui.View):
    def __init__(self, guild, cfg):
        super().__init__(timeout=300)
        self.guild = guild
        self.cfg   = cfg

    @discord.ui.button(label="Créer un type", style=discord.ButtonStyle.success, emoji="➕", row=0)
    async def create_type(self, inter: discord.Interaction, btn: discord.ui.Button):
        if not is_staff(inter.user, self.guild.id):
            await inter.response.send_message(embed=_e_err("🚫"), ephemeral=True); return
        ok_cfg, msg_cfg = is_configured(self.guild.id)
        if not ok_cfg:
            await inter.response.send_message(embed=_e_warn("⚠️  Config incomplète", msg_cfg), ephemeral=True); return
        await inter.response.send_modal(TypeCreateModal(self.guild, self.cfg))

    @discord.ui.button(label="Modifier un type", style=discord.ButtonStyle.primary, emoji="✏️", row=0)
    async def edit_type(self, inter: discord.Interaction, btn: discord.ui.Button):
        if not is_staff(inter.user, self.guild.id):
            await inter.response.send_message(embed=_e_err("🚫"), ephemeral=True); return
        if not self.cfg["types"]:
            await inter.response.send_message(embed=_e_warn("⚠️", "Aucun type à modifier."), ephemeral=True); return
        opts = [discord.SelectOption(label=t["emoji"] + " " + t["label"], value=k)
                for k, t in self.cfg["types"].items()]
        sel = discord.ui.Select(placeholder="Choisir le type à modifier…", options=opts)
        cfg_ref = self.cfg; guild_ref = self.guild
        async def _pick(i2):
            key = i2.data["values"][0]
            await i2.response.send_modal(TypeEditModal(guild_ref, cfg_ref, key))
        sel.callback = _pick
        v2 = discord.ui.View(timeout=60); v2.add_item(sel)
        await inter.response.send_message(embed=discord.Embed(title="✏️  Modifier quel type ?", color=C_PURPLE),
                                          view=v2, ephemeral=True)

    @discord.ui.button(label="Supprimer un type", style=discord.ButtonStyle.danger, emoji="🗑️", row=0)
    async def delete_type(self, inter: discord.Interaction, btn: discord.ui.Button):
        if not is_staff(inter.user, self.guild.id):
            await inter.response.send_message(embed=_e_err("🚫"), ephemeral=True); return
        if not self.cfg["types"]:
            await inter.response.send_message(embed=_e_warn("⚠️", "Aucun type à supprimer."), ephemeral=True); return
        opts = [discord.SelectOption(label=t["emoji"] + " " + t["label"], value=k)
                for k, t in self.cfg["types"].items()]
        sel = discord.ui.Select(placeholder="Choisir le type à supprimer…", options=opts)
        cfg_ref = self.cfg
        async def _del(i2):
            key = i2.data["values"][0]
            label = cfg_ref["types"][key]["label"]
            del cfg_ref["types"][key]; await save_data()
            await i2.response.send_message(embed=_e_ok("🗑️  Type supprimé", "**" + label + "** supprimé."), ephemeral=True)
        sel.callback = _del
        v2 = discord.ui.View(timeout=60); v2.add_item(sel)
        await inter.response.send_message(embed=discord.Embed(title="🗑️  Supprimer quel type ?", color=C_RED),
                                          view=v2, ephemeral=True)


class TypeCreateModal(discord.ui.Modal, title="➕  Créer un type de ticket"):
    t_key   = discord.ui.TextInput(label="Clé interne (ex: support)",     placeholder="support",       max_length=20)
    t_label = discord.ui.TextInput(label="Nom affiché",                    placeholder="Support technique", max_length=40)
    t_emoji = discord.ui.TextInput(label="Emoji",                          placeholder="🎫",            max_length=4,  required=False)
    t_qs    = discord.ui.TextInput(label="Questions (une par ligne)",
                                    placeholder="Décris ton problème\nDepuis quand ?",
                                    style=discord.TextStyle.paragraph,     max_length=1000, required=False)

    def __init__(self, guild, cfg):
        super().__init__(); self.guild = guild; self.cfg = cfg

    async def on_submit(self, inter: discord.Interaction):
        key   = self.t_key.value.strip().lower().replace(" ", "_")
        label = self.t_label.value.strip()
        emoji = self.t_emoji.value.strip() or "🎫"
        qs    = [q.strip() for q in self.t_qs.value.splitlines() if q.strip()]
        if not key or not label:
            await inter.response.send_message(embed=_e_err("❌  Clé et nom requis"), ephemeral=True); return
        self.cfg["types"][key] = {"label": label, "emoji": emoji, "questions": qs}
        await save_data()
        desc = emoji + " **" + label + "**  `[" + key + "]`\n" + str(len(qs)) + " question(s)"
        await inter.response.send_message(embed=_e_ok("✅  Type créé", desc), ephemeral=True)


class TypeEditModal(discord.ui.Modal, title="✏️  Modifier un type de ticket"):
    t_label = discord.ui.TextInput(label="Nom affiché",   max_length=40)
    t_emoji = discord.ui.TextInput(label="Emoji",         max_length=4, required=False)
    t_qs    = discord.ui.TextInput(label="Questions (une par ligne)",
                                    style=discord.TextStyle.paragraph, max_length=1000, required=False)

    def __init__(self, guild, cfg, key):
        super().__init__()
        self.guild = guild; self.cfg = cfg; self.key = key
        t = cfg["types"][key]
        self.t_label.default = t["label"]
        self.t_emoji.default = t["emoji"]
        self.t_qs.default    = "\n".join(t.get("questions", []))

    async def on_submit(self, inter: discord.Interaction):
        label = self.t_label.value.strip() or self.cfg["types"][self.key]["label"]
        emoji = self.t_emoji.value.strip() or self.cfg["types"][self.key]["emoji"]
        qs    = [q.strip() for q in self.t_qs.value.splitlines() if q.strip()]
        self.cfg["types"][self.key] = {"label": label, "emoji": emoji, "questions": qs}
        await save_data()
        await inter.response.send_message(embed=_e_ok("✅  Type mis à jour", emoji + " **" + label + "**  — " + str(len(qs)) + " question(s)"), ephemeral=True)


# ── Vue : membres ────────────────────────────────────────────────
class MemberActionView(discord.ui.View):
    def __init__(self, guild, page=0):
        super().__init__(timeout=120)
        self.guild = guild
        self.page  = page
        humans = [m for m in guild.members if not m.bot]
        start  = page * 24
        chunk  = humans[start:start + 24]
        has_next = (start + 24) < len(humans)
        si = {discord.Status.online: "🟢", discord.Status.idle: "🟡",
              discord.Status.dnd: "🔴", discord.Status.offline: "⚫"}
        opts = [discord.SelectOption(
            label=(si.get(m.status, "⚫") + " " + m.display_name)[:50],
            description="ID: " + str(m.id),
            value=str(m.id)) for m in chunk]
        if has_next:
            opts.append(discord.SelectOption(
                label="➡️  Page suivante (" + str(len(humans) - start - 24) + " membres restants)",
                value="__next_page__",
                description="Voir plus de membres"))
        if opts:
            suffix = "  (p." + str(page + 1) + ")" if len(humans) > 24 else ""
            sel = discord.ui.Select(placeholder="👤  Choisir un membre" + suffix + "…", options=opts)
            sel.callback = self.on_select
            self.add_item(sel)

    async def on_select(self, inter: discord.Interaction):
        if not is_staff(inter.user, self.guild.id):
            await inter.response.send_message(embed=_e_err("🚫"), ephemeral=True); return
        val = inter.data["values"][0]
        if val == "__next_page__":
            await inter.response.edit_message(view=MemberActionView(self.guild, self.page + 1))
            return
        mem = self.guild.get_member(int(val))
        if not mem:
            await inter.response.send_message(embed=_e_err("❌  Membre introuvable"), ephemeral=True); return
        si  = {discord.Status.online: "🟢 En ligne", discord.Status.idle: "🟡 AFK",
               discord.Status.dnd: "🔴 DND", discord.Status.offline: "⚫ Hors ligne"}
        col = mem.top_role.color if mem.top_role and mem.top_role.color.value else discord.Color.default()
        e   = discord.Embed(title="👤  " + mem.display_name,
                            description="Membre depuis <t:" + str(int(mem.joined_at.timestamp())) + ":D>",
                            color=col)
        if mem.avatar: e.set_thumbnail(url=mem.avatar.url)
        e.add_field(name="📊  Statut",   value=si.get(mem.status, "⚫"), inline=True)
        e.add_field(name="🏷  Top rôle", value=mem.top_role.mention if mem.top_role else "—", inline=True)
        e.add_field(name="🆔  ID",       value="`" + str(mem.id) + "`", inline=True)
        roles_txt = " ".join(r.mention for r in reversed(mem.roles) if r.name != "@everyone")[:512] or "*Aucun*"
        e.add_field(name="📋  Rôles", value=roles_txt, inline=False)
        await inter.response.send_message(embed=e, view=MemberModView(self.guild, mem), ephemeral=True)


# ── Vue : actions membre (ticket, kick, ban) ─────────────────────
class MemberModView(discord.ui.View):
    def __init__(self, guild, member):
        super().__init__(timeout=60)
        self.guild  = guild
        self.member = member

    @discord.ui.button(label="Ouvrir un ticket", style=discord.ButtonStyle.primary, emoji="🎫")
    async def open_tkt(self, inter: discord.Interaction, btn: discord.ui.Button):
        if not is_staff(inter.user, self.guild.id):
            await inter.response.send_message(embed=_e_err("🚫"), ephemeral=True); return
        if maintenance_mode:
            await inter.response.send_message(embed=_e_warn("🔧  Maintenance active", "Les tickets sont désactivés."), ephemeral=True); return
        ok_cfg, msg_cfg = is_configured(self.guild.id)
        if not ok_cfg:
            await inter.response.send_message(embed=_e_warn("⚠️  Config incomplète", msg_cfg), ephemeral=True); return
        cfg = tconf(self.guild.id)
        if not cfg["types"]:
            await inter.response.send_message(embed=_e_warn("⚠️", "Aucun type configuré."), ephemeral=True); return
        opts = [discord.SelectOption(label=v["emoji"] + " " + v["label"], value=k)
                for k, v in cfg["types"].items()]
        sel  = discord.ui.Select(placeholder="Type de ticket…", options=opts)
        _g   = self.guild
        _m   = self.member

        async def _pick(i3):
            key = i3.data["values"][0]
            ch, err = await open_ticket(_g, _m, key)
            if err: await i3.response.send_message(embed=_e_err("❌  " + err), ephemeral=True)
            else:   await i3.response.send_message(embed=_e_ok("✅  Ticket créé", ch.mention), ephemeral=True)

        sel.callback = _pick
        v2 = discord.ui.View(timeout=30)
        v2.add_item(sel)
        await inter.response.send_message(view=v2, ephemeral=True)

    @discord.ui.button(label="Kick", style=discord.ButtonStyle.danger, emoji="👢")
    async def kick_btn(self, inter: discord.Interaction, btn: discord.ui.Button):
        if not is_staff(inter.user, self.guild.id):
            await inter.response.send_message(embed=_e_err("🚫"), ephemeral=True); return
        await inter.response.send_modal(ModModal(self.guild, self.member, "kick"))

    @discord.ui.button(label="Ban", style=discord.ButtonStyle.danger, emoji="🔨")
    async def ban_btn(self, inter: discord.Interaction, btn: discord.ui.Button):
        if not is_staff(inter.user, self.guild.id):
            await inter.response.send_message(embed=_e_err("🚫"), ephemeral=True); return
        await inter.response.send_modal(ModModal(self.guild, self.member, "ban"))


class ModModal(discord.ui.Modal):
    raison = discord.ui.TextInput(label="Raison", placeholder="Raison de la sanction…",
                                   style=discord.TextStyle.short, max_length=200, required=False)

    def __init__(self, guild, member, action):
        super().__init__(title=("👢 Kick" if action == "kick" else "🔨 Ban") + " — " + member.display_name)
        self.guild  = guild
        self.member = member
        self.action = action

    async def on_submit(self, inter: discord.Interaction):
        raison = self.raison.value or "Sanction par le staff."
        try:
            if self.action == "kick":
                await self.member.kick(reason=raison)
                await inter.response.send_message(
                    embed=_e_ok("👢  " + self.member.display_name + " expulsé", raison), ephemeral=True)
            else:
                await self.member.ban(reason=raison, delete_message_days=0)
                await inter.response.send_message(
                    embed=_e_ok("🔨  " + self.member.display_name + " banni", raison), ephemeral=True)
        except discord.Forbidden:
            await inter.response.send_message(embed=_e_err("❌  Permission refusée"), ephemeral=True)


# ── API HTTP ───────────────────────────────────────────────────────
def _vsig(body, sig):
    return hmac.compare_digest(hmac.new(REMOTE_SECRET.encode(), body, hashlib.sha256).hexdigest(), sig)

async def handle_health(req): return web.Response(text="ok")

async def handle_remote(req):
    if not REMOTE_ENABLED:
        return web.json_response({"error": "Remote disabled"}, status=403)
    body = await req.read()
    if not _vsig(body, req.headers.get("X-Signature", "")):
        return web.json_response({"error": "Unauthorized"}, status=401)
    # Parse du payload entrant
    try:
        p = json.loads(body)
    except json.JSONDecodeError as e:
        log("API", f"Payload JSON invalide : {e} — body brut : {body[:200]}")
        return web.json_response({"error": f"Invalid JSON: {e}"}, status=400)
    action = p.get("action", "")
    log("API", f"Action reçue : '{action}' — guild_id={p.get('guild_id')}")
    # Dispatch protégé — toujours renvoyer du JSON valide
    try:
        result = await dispatch(action, p)
        # Sérialiser pour détecter les objets non-sérialisables avant d'envoyer
        raw = json.dumps(result)
        return web.Response(
            text=raw,
            content_type="application/json"
        )
    except (TypeError, ValueError) as e:
        log("API", f"Erreur sérialisation réponse action='{action}' : {e}")
        return web.json_response({"error": f"Serialization error: {e}"}, status=500)
    except Exception as e:
        import traceback
        log("API", f"Exception non gérée action='{action}' : {e}")
        log("API", traceback.format_exc())
        return web.json_response({"error": f"Internal error: {e}"}, status=500)

def _g(p):
    gid = p.get("guild_id")
    if gid: return client.get_guild(gid)
    return client.guilds[0] if client.guilds else None

async def dispatch(action, p):
    try:
        return await _dispatch_inner(action, p)
    except Exception as e:
        import traceback
        log("API", f"Exception dans dispatch action='{action}' : {e}")
        log("API", traceback.format_exc())
        return {"error": f"Dispatch error: {e}"}

async def _dispatch_inner(action, p):
    global maintenance_mode

    if action == "status":
        guilds = []
        for g in client.guilds:
            cfg = ticket_config.get(g.id, {})
            cat = g.get_channel(cfg.get("category_id") or 0)
            sch = g.get_channel(cfg.get("staff_channel_id") or 0)
            sro = g.get_role(cfg.get("staff_role_id") or 0)
            ok_cfg, _ = is_configured(g.id)
            guilds.append({"id": g.id, "name": g.name, "members": g.member_count,
                "category": cat.name if cat else None, "staff_channel": sch.name if sch else None,
                "staff_role": sro.name if sro else None,
                "ticket_types": list(cfg.get("types", {}).keys()), "counter": cfg.get("counter", 0),
                "menu_locked": cfg.get("menu_locked", False), "configured": ok_cfg})
        return {"bot": str(client.user), "guilds": guilds,
                "maintenance": maintenance_mode,
                "open_tickets": len([s for s in ticket_sessions.values() if s["status"] in ("open", "pending")]),
                "pending": len(pending_channels)}

    elif action == "maintenance":
        val = p.get("enabled")
        maintenance_mode = bool(val) if val is not None else (not maintenance_mode)
        await save_data()
        log("BOT", "maintenance_mode =", maintenance_mode)
        return {"maintenance": maintenance_mode, "ok": True}
    elif action == "lockmenu":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        tconf(g.id)["menu_locked"] = True; await save_data()
        return {"menu_locked": True, "guild": g.name}
    elif action == "unlockmenu":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        tconf(g.id)["menu_locked"] = False; await save_data()
        return {"menu_locked": False, "guild": g.name}

    elif action == "basecfg":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        cfg = tconf(g.id)
        for k in ("category_id", "staff_channel_id", "staff_role_id"):
            if k in p: cfg[k] = p[k]
        await save_data(); return {"ok": True}

    elif action == "ttype_list":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        return {"types": tconf(g.id)["types"]}

    elif action == "ttype_add":
        g = _g(p); key = p.get("key", "").lower()
        if not g: return {"error": "Guild not found"}
        if not key: return {"error": "key required"}
        ok_cfg, msg_cfg = is_configured(g.id)
        if not ok_cfg: return {"error": "Config incomplète : " + msg_cfg}
        cfg = tconf(g.id)
        cfg["types"][key] = {"label": p.get("label", key), "emoji": p.get("emoji", "🎫"), "questions": p.get("questions", [])}
        await save_data(); return {"ok": True, "key": key}

    elif action == "ttype_edit":
        g = _g(p); key = p.get("key", "").lower()
        if not g: return {"error": "Guild not found"}
        cfg = tconf(g.id)
        if key not in cfg["types"]: return {"error": "Type '" + key + "' not found"}
        for f in ("label", "emoji", "questions"):
            if f in p: cfg["types"][key][f] = p[f]
        await save_data(); return {"ok": True}

    elif action == "ttype_delete":
        g = _g(p); key = p.get("key", "").lower()
        if not g: return {"error": "Guild not found"}
        cfg = tconf(g.id)
        if key not in cfg["types"]: return {"error": "Type '" + key + "' not found"}
        del cfg["types"][key]; await save_data(); return {"ok": True}

    elif action == "topen":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        ok_cfg, msg_cfg = is_configured(g.id)
        if not ok_cfg: return {"error": msg_cfg}
        m = find_member(str(p.get("member", "")), g)
        if not m: return {"error": "Member not found"}
        type_key = str(p.get("type_key", "")).lower()
        ch, err = await open_ticket(g, m, type_key)
        if err: return {"error": err}
        cfg2 = tconf(g.id)
        # Si le type a des questions → lancer run_questions avec la session existante (pas de double compteur)
        if cfg2["types"].get(type_key, {}).get("questions"):
            s = ticket_sessions.get(ch.id)
            if s:
                asyncio.create_task(run_questions(g, cfg2, ch, m, type_key, existing_session=s))
                return {"ok": True, "channel": ch.name, "channel_id": ch.id, "questions": True}
        return {"ok": True, "channel": ch.name, "channel_id": ch.id, "questions": False}

    elif action == "tlaunch":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        ok_cfg, msg_cfg = is_configured(g.id)
        if not ok_cfg: return {"error": msg_cfg}
        cfg = tconf(g.id)
        cid = p.get("channel_id")
        ch  = g.get_channel(int(cid)) if cid else find_channel(str(p.get("channel", "")), g)
        if not ch: return {"error": "Channel not found"}
        key = str(p.get("type_key", "")).lower()
        if key not in cfg["types"]: return {"error": "Type '" + key + "' not found"}
        # Priorité : member_id explicite → owner_id stocké dans pending → premier non-admin dans salon
        member = None
        explicit_mid = p.get("member_id")
        if explicit_mid:
            member = g.get_member(int(explicit_mid))
        if not member:
            owner_id = pending_channels.get(ch.id, {}).get("owner_id")
            if owner_id:
                member = g.get_member(owner_id)
        if not member:
            membres = [m for m in g.members if not m.bot and ch.permissions_for(m).view_channel
                       and not m.guild_permissions.administrator]
            if not membres:
                membres = [m for m in g.members if not m.bot and ch.permissions_for(m).view_channel]
            if not membres: return {"error": "No member in channel"}
            member = membres[0]
        asyncio.create_task(run_questions(g, cfg, ch, member, key))
        return {"ok": True, "channel": ch.name, "member": member.display_name, "member_id": member.id}

    elif action in ("taccept", "trefuse", "tclose"):
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        try: num = int(str(p.get("number", "")).lstrip("#"))
        except: return {"error": "Invalid number"}
        s = next((x for x in ticket_sessions.values() if x["guild_id"] == g.id and x["number"] == num), None)
        if not s: return {"error": "Ticket #" + str(num) + " not found"}
        act    = {"taccept": "accepted", "trefuse": "refused", "tclose": "closed"}[action]
        raison = str(p.get("raison", ""))
        await do_action(g, s, act, raison); return {"ok": True, "number": num, "action": act}

    elif action == "tsend":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        try: num = int(str(p.get("number", "")).lstrip("#"))
        except: return {"error": "Invalid number"}
        s = next((x for x in ticket_sessions.values() if x["guild_id"] == g.id and x["number"] == num), None)
        if not s: return {"error": "Ticket #" + str(num) + " not found"}
        ch = g.get_channel(s["channel_id"])
        if not ch: return {"error": "Channel not found"}
        await ch.send("📢 **Staff** : " + p.get("message", "")); return {"ok": True}

    elif action == "tlist":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        f  = p.get("filter", "all")
        ss = [s for s in ticket_sessions.values() if s["guild_id"] == g.id]
        if f == "open":   ss = [s for s in ss if s["status"] in ("open", "pending")]
        elif f == "closed": ss = [s for s in ss if s["status"] not in ("open", "pending")]
        return {"tickets": [{"number": s["number"], "type": s["type"], "status": s["status"],
                              "user_id": s["user_id"], "opened_at": s["opened_at"]} for s in ss]}

    elif action == "tpending":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        result = []
        for i, (k, v) in enumerate([(k, v) for k, v in pending_channels.items() if v["guild_id"] == g.id], 1):
            ch = g.get_channel(k)
            result.append({"index": i, "channel_id": k, "name": ch.name if ch else str(k), "detected_at": v["detected_at"]})
        return {"pending": result}

    elif action == "guilds":
        return {"guilds": [{"id": g.id, "name": g.name, "members": g.member_count} for g in client.guilds]}

    elif action == "members":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        return {"members": [{"index": i, "id": m.id, "name": m.display_name, "status": str(m.status)}
                             for i, m in enumerate([m for m in g.members if not m.bot], 1)]}

    elif action == "roles":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        return {"roles": [{"index": i, "id": r.id, "name": r.name, "members": len(r.members)}
                           for i, r in enumerate([r for r in g.roles if r.name != "@everyone"], 1)]}

    elif action == "channels":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        return {"channels": [{"index": i, "id": c.id, "name": c.name}
                              for i, c in enumerate([c for c in g.channels if isinstance(c, discord.TextChannel)], 1)]}

    elif action == "categories":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        return {"categories": [{"index": i, "id": c.id, "name": c.name, "channels": len(c.channels)}
                                for i, c in enumerate(sorted(g.categories, key=lambda x: x.position), 1)]}

    elif action == "send":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        ch = find_channel(str(p.get("channel", "")), g)
        if not ch: return {"error": "Channel not found"}
        content = str(p.get("message", ""))
        mid = p.get("mention_id")
        if mid:
            m = g.get_member(int(mid))
            if m: content = m.mention + " " + content
        await ch.send(content); return {"ok": True}

    elif action == "read_messages":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        ch = find_channel(str(p.get("channel", "")), g)
        if not ch: return {"error": "Channel not found"}
        limit = int(p.get("limit", 20))
        try:
            msgs = []
            async for msg in ch.history(limit=min(limit, 50)):
                msgs.append({"id": msg.id, "author": msg.author.display_name,
                             "author_id": msg.author.id, "content": msg.content,
                             "timestamp": msg.created_at.strftime("%d/%m/%Y %H:%M:%S"),
                             "bot": msg.author.bot})
            return {"messages": msgs, "channel": ch.name}
        except discord.Forbidden:
            return {"error": "Permission refusée pour lire ce salon"}

    elif action == "accept_actions":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        try: num = int(str(p.get("number", "")).lstrip("#"))
        except: return {"error": "Invalid number"}
        s = next((x for x in ticket_sessions.values() if x["guild_id"] == g.id and x["number"] == num), None)
        if not s: return {"error": "Ticket #" + str(num) + " not found"}
        actions = {k: v for k, v in p.items() if k in ("rename", "invite", "add_role", "create_role")}
        errs = await do_accept_actions(g, s, actions)
        return {"ok": True, "errors": errs}

    elif action == "kick":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        m = find_member(str(p.get("member", "")), g)
        if not m: return {"error": "Member not found"}
        await m.kick(reason=p.get("reason", "Console")); return {"ok": True, "kicked": m.display_name}

    elif action == "ban":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        m = find_member(str(p.get("member", "")), g)
        if not m: return {"error": "Member not found"}
        await m.ban(reason=p.get("reason", "Console"), delete_message_days=0); return {"ok": True, "banned": m.display_name}


    # ── Musique ──────────────────────────────────────────────────
    elif action == "music_join":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        ch_id = p.get("channel_id")
        ch    = g.get_channel(ch_id) if ch_id else None
        if not ch or not isinstance(ch, discord.VoiceChannel):
            return {"error": "Salon vocal introuvable"}
        vc = g.voice_client
        if vc and vc.is_connected():
            await vc.move_to(ch)
        else:
            await ch.connect()
        return {"ok": True, "channel": ch.name}

    elif action == "music_play":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        if not SPOTIPY_OK:
            return {"error": "spotipy non installé"}
        # Vérifier que le bot est dans un vocal avant de chercher
        vc = g.voice_client
        if not vc or not vc.is_connected():
            return {"error": "Bot non connecté à un salon vocal — lance music_join d'abord"}
        query     = p.get("query", "")
        ch_id     = p.get("np_channel_id")
        requester = p.get("requester", "Console")
        if not query: return {"error": "query requis"}
        log("MUSIC", f"[Console] music_play query='{query[:60]}'", flush=True)
        tracks = await _resolve_query(query)
        if not tracks: return {"error": "Aucun résultat pour cette recherche"}
        for t in tracks: t["requester"] = requester
        st = _mstate(g.id)
        st["np_channel"] = ch_id
        was_empty = not st["queue"] and not (vc.is_playing() or vc.is_paused())
        st["queue"].extend(tracks)
        log("MUSIC", f"[Console] {len(tracks)} track(s) ajoutée(s), was_empty={was_empty}", flush=True)
        if was_empty and not vc.is_playing():
            await _play_next(g)
        return {"ok": True, "added": len(tracks),
                "titles": [t.get("title") or t.get("url", "?")[:60] for t in tracks[:5]]}

    elif action == "music_stop":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        vc = g.voice_client
        if vc and vc.is_connected():
            _mstate(g.id)["queue"].clear()
            _mstate(g.id)["current"] = None
            await vc.disconnect()
        return {"ok": True}

    elif action == "music_skip":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        vc = g.voice_client
        if not vc or not vc.is_playing(): return {"error": "Rien en lecture"}
        cur = _mstate(g.id).get("current", {})
        vc.stop()
        return {"ok": True, "skipped": cur.get("title", "?")}

    elif action == "music_pause":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        vc = g.voice_client
        if not vc or not vc.is_playing(): return {"error": "Rien en lecture"}
        vc.pause()
        return {"ok": True}

    elif action == "music_resume":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        vc = g.voice_client
        if not vc or not vc.is_paused(): return {"error": "Pas en pause"}
        vc.resume()
        return {"ok": True}

    elif action == "music_queue":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        st  = _mstate(g.id)
        return {
            "current": st.get("current"),
            "queue":   st.get("queue", []),
            "volume":  int(st.get("volume", 0.5) * 100),
        }

    elif action == "music_volume":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        level = int(p.get("level", 50))
        level = max(0, min(100, level))
        st    = _mstate(g.id)
        st["volume"] = level / 100
        vc = g.voice_client
        if vc and vc.source and isinstance(vc.source, discord.PCMVolumeTransformer):
            vc.source.volume = level / 100
        tconf(g.id)["music_volume"] = level
        await save_data()
        return {"ok": True, "volume": level}

    elif action == "music_lock":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        cfg = tconf(g.id)
        locked  = bool(p.get("locked", True))
        msg     = str(p.get("message", "")) .strip()
        cfg["music_locked"]   = locked
        cfg["music_lock_msg"] = msg
        await save_data()
        log("MUSIC", f"[{g.name}] Musique {'verrouillée' if locked else 'déverrouillée'}"
            + (f" — '{msg}'" if msg else ""), flush=True)
        return {"ok": True, "locked": locked, "message": msg}

    elif action == "music_cfg":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        cfg = tconf(g.id)
        if "music_enabled" in p:
            cfg["music_enabled"] = bool(p["music_enabled"])
        if "music_role_id" in p:
            cfg["music_role_id"] = p["music_role_id"]
        if "music_volume" in p:
            cfg["music_volume"]  = int(p["music_volume"])
        await save_data()
        return {"ok": True}

    elif action == "music_vclist":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        vcs = [{"id": c.id, "name": c.name, "members": len(c.members)}
               for c in g.channels if isinstance(c, discord.VoiceChannel)]
        return {"voice_channels": vcs}

    # ── Système de publication DEV → PROD ──────────────────────────────
    elif action == "update_check":
        # Comparer les commits de dev et main via git log
        # IMPORTANT : Render clone en mode shallow (--depth=1) sur main uniquement.
        # origin/dev n'existe pas dans ce clone sans fetch explicite.
        # Solution : fetcher dev explicitement avant de comparer.
        def _git(cmd):
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=15, cwd=BOT_DIR)
            return r.stdout.strip(), r.returncode

        current, _ = _git(["git", "rev-parse", "--abbrev-ref", "HEAD"])

        # Étape 1 : fetcher main ET dev explicitement
        # (sur un clone shallow Render, seul main est tracké par défaut)
        _git(["git", "fetch", "origin", "main", "--quiet"])
        _, rc_dev = _git(["git", "fetch", "origin", "dev", "--quiet"])

        if rc_dev != 0:
            # La branche dev n'existe pas ou est inaccessible
            return {
                "ok":            True,
                "is_up_to_date": False,
                "ahead":         0,
                "commits":       [],
                "current_branch": current,
                "changelog":     _cached_changelog,
                "warning":       "Branche 'dev' introuvable sur origin — as-tu pushé sur dev ?",
            }

        # Étape 2 : comparer les SHAs
        sha_main, _ = _git(["git", "rev-parse", "origin/main"])
        sha_dev,  _ = _git(["git", "rev-parse", "origin/dev"])

        if sha_main == sha_dev:
            # Même commit → vraiment à jour
            return {
                "ok":            True,
                "is_up_to_date": True,
                "ahead":         0,
                "commits":       [],
                "current_branch": current,
                "changelog":     _cached_changelog,
            }

        # Étape 3 : lister les commits de dev absents de main
        ahead_log, _ = _git(["git", "log", "origin/main..origin/dev", "--oneline", "--no-merges"])
        ahead_commits = [l.strip() for l in ahead_log.splitlines() if l.strip()]

        return {
            "ok":            True,
            "is_up_to_date": len(ahead_commits) == 0 and sha_main == sha_dev,
            "ahead":         len(ahead_commits),
            "commits":       ahead_commits[:20],
            "current_branch": current,
            "changelog":     _cached_changelog,
        }

    elif action == "update_publish":
        # Enregistrer le changelog en BDD
        version     = str(p.get("version", "")).strip()
        description = str(p.get("description", "")).strip()
        if not version or not description:
            return {"error": "version et description requis"}
        ok = await save_changelog(version, description)
        return {"ok": ok, "version": version, "description": description}

    elif action == "anon_config":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        cfg = tconf(g.id)
        if "enabled" in p:
            cfg["anon_enabled"] = bool(p["enabled"])
            await save_data()
        return {"ok": True, "anon_enabled": cfg.get("anon_enabled", True)}

    elif action == "activity_get":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        mid = p.get("member_id")
        if mid:
            m = g.get_member(int(mid))
            if not m: return {"error": "Member not found"}
            d = activity_data.get(g.id, {}).get(m.id, {})
            score = _inactivity_score(g.id, m.id)
            emoji, label = _score_label(score)
            return {"ok": True, "member": m.display_name, "score": score, "label": label,
                    "msg_count": d.get("msg_count",0), "voice_seconds": _total_voice(g.id, m.id),
                    "last_seen": _fmt_last_seen(d.get("last_seen")),
                    "exempt": _is_exempt(m, g.id)}
        # Liste complète
        humans = [m for m in g.members if not m.bot]
        result = []
        for m in humans:
            d = activity_data.get(g.id, {}).get(m.id, {})
            score = _inactivity_score(g.id, m.id)
            emoji, label = _score_label(score)
            result.append({"id": m.id, "name": m.display_name, "score": score,
                            "label": label, "emoji": emoji,
                            "msg_count": d.get("msg_count",0),
                            "voice_seconds": _total_voice(g.id, m.id),
                            "last_seen": _fmt_last_seen(d.get("last_seen")),
                            "exempt": _is_exempt(m, g.id)})
        result.sort(key=lambda x: x["score"], reverse=True)
        return {"ok": True, "members": result}

    elif action == "activity_exempt":
        g = _g(p)
        if not g: return {"error": "Guild not found"}
        cfg = tconf(g.id)
        role_ids = p.get("role_ids", [])
        cfg["activity_exempt_role_ids"] = [int(r) for r in role_ids]
        await save_data()
        return {"ok": True, "exempt_count": len(role_ids)}

    return {"error": "Unknown action: " + action}


# ── Lancement ──────────────────────────────────────────────────────
async def main():
    import traceback as _tb

    log("BOOT", "=" * 52)
    log("BOOT", f"bot_agent.py  v{BOT_VERSION}")
    log("BOOT", "=" * 52)
    log("BOOT", f"Python         : {sys.version.split()[0]}")
    log("BOOT", f"discord.py     : {discord.__version__}")
    log("BOOT", "Musique        : Spotify (métadonnées) + Deezer (audio)")
    log("BOOT", f"spotipy        : {'OK' if SPOTIPY_OK else 'non installe (optionnel)'}")
    log("BOOT", f"FFmpeg chemin  : {FFMPEG_EXECUTABLE}")
    # ── Test FFmpeg réel : vérifier qu'il existe ET fonctionne ──────
    try:
        _ffv = subprocess.run(
            [FFMPEG_EXECUTABLE, "-version"],
            capture_output=True, text=True, timeout=5
        )
        if _ffv.returncode == 0:
            _ffv_line = _ffv.stdout.split("\n")[0]
            log("BOOT", f"FFmpeg test    : OK ✔ — {_ffv_line[:70]}")
        else:
            log("BOOT", f"FFmpeg test    : ECHEC returncode={_ffv.returncode} ❌")
            log("BOOT", f"FFmpeg stderr  : {_ffv.stderr[:200]}")
    except FileNotFoundError:
        log("BOOT", f"FFmpeg test    : INTROUVABLE a '{FFMPEG_EXECUTABLE}' ❌")
        log("BOOT",  "                 -> La lecture audio sera impossible !")
        log("BOOT",  "                 -> Verifier RUN apt-get install -y ffmpeg dans le Dockerfile")
    except subprocess.TimeoutExpired:
        log("BOOT", "FFmpeg test    : timeout 5s ❌")
    except Exception as _eff:
        log("BOOT", f"FFmpeg test    : exception : {_eff}")
    # ── Test PyNaCl : OBLIGATOIRE pour le son Discord ───────────────
    # Sans PyNaCl, vc.play() s'execute sans erreur mais n'envoie AUCUN paquet audio
    try:
        import nacl as _nacl_mod
        _nacl_ver = getattr(_nacl_mod, "__version__", "?")
        log("BOOT", f"PyNaCl         : OK ✔ v{_nacl_ver}")
    except ImportError:
        log("BOOT", "PyNaCl         : MANQUANT ❌ — vc.play() ne transmettra AUCUN son !")
        log("BOOT", "                 -> Ajouter PyNaCl>=1.5.0 dans requirements.txt")
    # ── Test Opus au démarrage ───────────────────────────────────────────
    # discord.py charge Opus automatiquement dans OpusEncoder mais seulement
    # au moment du premier send_audio_packet(). On anticipe ici pour diagnostic.
    try:
        _opus_loaded = discord.opus.is_loaded()
        if not _opus_loaded:
            for _olib in ("libopus.so.0", "libopus.so", "opus", "libopus-0.dll"):
                try:
                    discord.opus.load_opus(_olib)
                    _opus_loaded = discord.opus.is_loaded()
                    if _opus_loaded:
                        log("BOOT", f"Opus           : chargé ✔ via {_olib}")
                        break
                except Exception:
                    continue
        if _opus_loaded:
            log("BOOT", "Opus           : OK ✔ (encodage vocal Discord disponible)")
        else:
            log("BOOT", "Opus           : NON CHARGÉ ❌")
            log("BOOT", "                 Sans Opus, vc.play() sera silencieux !")
            log("BOOT", "                 -> apt-get install -y libopus0  dans le Dockerfile")
    except Exception as _e_o:
        log("BOOT", f"Opus           : exception chargement : {_e_o}")
    # ── Options audio actives ────────────────────────────────────────
    log("BOOT", f"FFMPEG opts    : before='{FFMPEG_BEFORE_OPTIONS}' / options='{FFMPEG_OPTIONS}'")
    if SPOTIPY_OK and SPOTIFY_CLIENT_ID:
        log("BOOT", "Spotify        : OK ✔ (métadonnées + recherche + playlists)")
        log("BOOT", f"Deezer ARL     : {'OK ✔ (lecture complète)' if os.getenv('DEEZER_ARL') else 'absent → previews 30s uniquement'}")
    log("BOOT", f"PORT           : {PORT}")
    log("BOOT", f"REMOTE_ENABLED : {REMOTE_ENABLED}")
    log("BOOT", "DISCORD_TOKEN  : " + ("OK" if TOKEN else "*** MANQUANT ***"))
    log("BOOT", "DATABASE_URL   : " + ("OK (" + DATABASE_URL.split("@")[-1] + ")" if DATABASE_URL else "non definie — mode memoire"))
    log("BOOT", "SPOTIFY        : " + ("OK" if (SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET) else "non configure (optionnel)"))

    if not TOKEN:
        log("BOOT", "FATAL : DISCORD_TOKEN manquant — impossible de se connecter.")
        log("BOOT", "Ajoute DISCORD_TOKEN dans les variables d'environnement Render.")
        sys.exit(1)

    try:
        log("HTTP", "Demarrage serveur HTTP...")
        app = web.Application()
        app.router.add_get("/health", handle_health)
        app.router.add_post("/remote", handle_remote)
        runner = web.AppRunner(app)
        await runner.setup()
        await web.TCPSite(runner, "0.0.0.0", PORT).start()
        log("HTTP", f"Serveur HTTP actif sur 0.0.0.0:{PORT}")
    except Exception as e:
        log("BOOT", f"FATAL : impossible de demarrer le serveur HTTP : {e}")
        log("BOOT", _tb.format_exc())
        sys.exit(1)

    try:
        log("BOT", "Connexion a Discord...")
        await client.start(TOKEN)
    except discord.LoginFailure as e:
        log("BOT", f"FATAL : token Discord invalide : {e}")
        log("BOT", "Verifie la variable DISCORD_TOKEN dans Render.")
        sys.exit(1)
    except Exception as e:
        log("BOT", f"FATAL : erreur connexion Discord : {e}")
        log("BOT", _tb.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    import traceback as _tb_main
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("BOOT", "Arret demande (KeyboardInterrupt).")
    except Exception as e:
        log("BOOT", f"EXCEPTION NON GEREE AU DEMARRAGE : {e}")
        log("BOOT", _tb_main.format_exc())
        sys.exit(1)
