#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
  BOT AGENT v5.0 — hébergé sur Render
  Variables : DISCORD_TOKEN  BOT_REMOTE_SECRET  BOT_REMOTE_ENABLED  PORT

  v5.2 : /menu slash command, persistance PostgreSQL, boutons désactivés
         après action ticket, menu_locked console-only, système musique complet
"""
import discord, asyncio, aiohttp, json, os, sys, hmac, hashlib, subprocess
import asyncpg
try:
    import yt_dlp as youtube_dl
    YT_DLP_OK = True
except ImportError:
    YT_DLP_OK = False
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

def log(tag: str, *args):
    """Log horodate avec flush immediat — visible dans Render sans buffer."""
    from datetime import datetime
    ts  = datetime.now().strftime("%H:%M:%S")
    msg = " ".join(str(a) for a in args)
    print(f"[{ts}] [{tag}] {msg}", flush=True)

TOKEN          = os.getenv("DISCORD_TOKEN", "")
REMOTE_SECRET  = os.getenv("BOT_REMOTE_SECRET", "changeme")
REMOTE_ENABLED = os.getenv("BOT_REMOTE_ENABLED", "true").lower() == "true"
PORT           = int(os.getenv("PORT", 8080))
DATA_FILE      = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot_data.json")
DATABASE_URL   = os.getenv("DATABASE_URL", "")   # URL PostgreSQL Supabase
_db_pool       = None   # pool de connexions asyncpg (initialisé au démarrage)

BOT_VERSION    = "5.2"  # version affichée dans le message de mise à jour

# ── Spotify credentials (optionnel) ──────────────────────────
SPOTIFY_CLIENT_ID     = os.getenv("SPOTIFY_CLIENT_ID", "")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET", "")

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
activity_tracker = defaultdict(dict)
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
                        "types":         cfg.get("types", {}),
                        "music_enabled": cfg.get("music_enabled", True),
                        "music_role_id": cfg.get("music_role_id"),
                        "music_volume":  cfg.get("music_volume", 50),
                    }, ensure_ascii=False))
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
                    "category_id":      row["category_id"],
                    "staff_channel_id": row["staff_channel_id"],
                    "staff_role_id":    row["staff_role_id"],
                    "counter":          row["counter"],
                    "menu_locked":      bool(row["menu_locked"]),
                    "types":            types_data,
                    "music_enabled":    music_enabled_data,
                    "music_role_id":    music_role_data,
                    "music_volume":     music_volume_data,
                }
            rows = await conn.fetch("SELECT * FROM ticket_session")
            for row in rows:
                ticket_sessions[row["channel_id"]] = json.loads(row["data_json"])
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
        }
    if "menu_locked" not in ticket_config[gid]:
        ticket_config[gid]["menu_locked"] = False
    # Migrations des champs musique manquants (serveurs existants)
    cfg = ticket_config[gid]
    if "music_enabled" not in cfg: cfg["music_enabled"] = True
    if "music_role_id" not in cfg: cfg["music_role_id"] = None
    if "music_volume"  not in cfg: cfg["music_volume"]  = 50
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

# ── Événements Discord ─────────────────────────────────────────────
def get_last_commit_message() -> str:
    """Lit le dernier message de commit Git via subprocess."""
    try:
        result = subprocess.run(
            ["git", "log", "-1", "--pretty=%B"],
            capture_output=True, text=True, timeout=5
        )
        msg = result.stdout.strip()
        return msg if msg else "Mise à jour sans description de commit."
    except Exception as e:
        log("BOT", f"get_last_commit_message erreur : {e}")
        return "Mise à jour déployée."


async def send_update_notifications():
    """Envoie un embed de mise à jour dans chaque salon staff configuré.
    Appelé une seule fois par démarrage, après load_data().
    """
    commit_msg = get_last_commit_message()
    log("BOT", f"Notification mise à jour — commit : {commit_msg[:60]}")

    sent = 0
    for g in client.guilds:
        cfg = ticket_config.get(g.id)
        if not cfg or not cfg.get("staff_channel_id"):
            continue
        ch = g.get_channel(cfg["staff_channel_id"])
        if not ch:
            log("BOT", f"[{g.name}] salon staff introuvable (id={cfg['staff_channel_id']})")
            continue
        # Vérifier la permission d'envoyer
        if not ch.permissions_for(g.me).send_messages:
            log("BOT", f"[{g.name}] permission refusée dans #{ch.name}")
            continue
        try:
            e = discord.Embed(
                title="✅  Bot mis à jour",
                description=commit_msg,
                color=C_GREEN,
                timestamp=datetime.now(timezone.utc)
            )
            e.add_field(name="🔖  Version", value=f"`{BOT_VERSION}`", inline=True)
            e.add_field(name="🌐  Serveur", value=g.name, inline=True)
            e.set_footer(text=client.user.name)
            await ch.send(embed=e)
            sent += 1
            log("BOT", f"[{g.name}] notification envoyée dans #{ch.name}")
        except Exception as ex:
            log("BOT", f"[{g.name}] erreur envoi notification : {ex}")

    log("BOT", f"Notifications envoyées : {sent}/{len(client.guilds)} serveur(s)")


@client.event
async def on_ready():
    log("BOT", "Connecte en tant que", str(client.user))
    log("BOT", f"{len(client.guilds)} serveur(s) visibles")
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
        music_cmds = {
            "play": slash_play, "stop": slash_stop, "skip": slash_skip,
            "pause": slash_pause, "resume": slash_resume,
            "queue": slash_queue, "volume": slash_volume,
        }
        for name, fn in {"menu": slash_menu, **music_cmds}.items():
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
        activity_tracker[message.guild.id][message.author.id] = datetime.now(timezone.utc)
    await handle_cmds(message)

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
#  MOTEUR MUSIQUE  v2 — pipeline entièrement revu
#  Utilise yt_dlp + discord.py voice + FFmpeg
#
#  BUGS CORRIGÉS vs v1 :
#  1. extract_flat="in_playlist" dans YTDL_OPTS héritait dans _get_stream_url
#     → yt_dlp retournait une URL de page HTML au lieu d'un stream CDN audio
#     → FFmpegPCMAudio échouait silencieusement → after() → _play_next → queue
#     vide → asyncio.sleep(2) → disconnect  [cause du "rejoint puis quitte"]
#  2. asyncio.get_event_loop() deprecated Python 3.10+ → get_running_loop()
#  3. autocomplete copiait YTDL_OPTS → extract_flat → webpage_url absente
#     → 0 suggestions retournées
#  4. Recherche texte via YTDL_OPTS → extract_flat → pas de stream résolvable
#
#  ARCHITECTURE :
#  - YTDL_SEARCH_OPTS  : recherche rapide (titre + webpage_url seulement)
#  - YTDL_STREAM_OPTS  : résolution d'une URL → stream CDN audio (extract_flat=False OBLIGATOIRE)
#  - YTDL_FLAT_OPTS    : listing de playlist (flat, sans résoudre chaque vidéo)
# ══════════════════════════════════════════════════════════════════

import shutil as _shutil

# ── Chemin FFmpeg ─────────────────────────────────────────────────
FFMPEG_EXECUTABLE = _shutil.which("ffmpeg") or "/usr/bin/ffmpeg"
log("MUSIC", f"FFmpeg : {FFMPEG_EXECUTABLE}")

FFMPEG_OPTS = {
    "executable":     FFMPEG_EXECUTABLE,
    "before_options": (
        "-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 "
        "-loglevel warning"
    ),
    "options": "-vn -bufsize 128k",
}

# ── Options yt_dlp séparées par usage ────────────────────────────
#
# YTDL_SEARCH_OPTS — autocomplete & résolution texte/URL directe
#   extract_flat=True → rapide, retourne titre + webpage_url + id
#   NE PAS utiliser pour obtenir un stream audio
#
_YTDL_BASE = {
    "format":         "bestaudio/best",
    "quiet":          True,
    "no_warnings":    True,
    "source_address": "0.0.0.0",
    "socket_timeout": 15,
}

YTDL_SEARCH_OPTS = {
    **_YTDL_BASE,
    "extract_flat": True,
    "noplaylist":   True,
}

# YTDL_STREAM_OPTS — résolution d'une webpage_url en vrai stream CDN audio
#   extract_flat=False OBLIGATOIRE — c'est l'option qui débloque l'URL audio
#   Sans ça, yt_dlp retourne la page web au lieu du stream → FFmpeg plante
#
YTDL_STREAM_OPTS = {
    **_YTDL_BASE,
    "extract_flat": False,   # ← FIX CRITIQUE
    "noplaylist":   True,
}

# YTDL_FLAT_OPTS — listing de playlist YouTube (sans résoudre chaque vidéo)
YTDL_FLAT_OPTS = {
    **_YTDL_BASE,
    "extract_flat": "in_playlist",
    "noplaylist":   False,
}


# ── État musique par serveur ──────────────────────────────────────
def _mstate(gid: int) -> dict:
    """Retourne (et initialise si besoin) l'état musique du serveur."""
    if gid not in music_state:
        music_state[gid] = {
            "queue":      [],
            "current":    None,
            "volume":     0.5,
            "np_channel": None,
        }
    return music_state[gid]

def _can_use_music(member: discord.Member, gid: int) -> tuple[bool, str]:
    """Vérifie si member peut utiliser les commandes musique."""
    cfg = tconf(gid)
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


# ── Résolution de requête → liste de tracks ───────────────────────
async def _resolve_query(query: str) -> list[dict]:
    """
    Résout une requête en liste de tracks.
    Track = {"title", "url" (webpage_url), "duration", "thumbnail"}
    L'URL audio stream est résolue plus tard par _get_stream_url().
    """
    log("MUSIC", f"Résolution query : {query[:80]}")

    # ── Playlist Spotify ─────────────────────────────────────────
    if "spotify.com/playlist/" in query and SPOTIPY_OK and SPOTIFY_CLIENT_ID:
        try:
            sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
                client_id=SPOTIFY_CLIENT_ID,
                client_secret=SPOTIFY_CLIENT_SECRET))
            results = sp.playlist_items(query, fields="items.track(name,artists)", limit=50)
            tracks = []
            for item in results.get("items", []):
                t = item.get("track")
                if not t: continue
                artist = t["artists"][0]["name"] if t.get("artists") else ""
                title  = f"{t['name']} {artist}".strip()
                # url=None → _get_stream_url fera une recherche YT au moment de jouer
                tracks.append({"title": title, "url": None, "duration": 0, "thumbnail": None})
            log("MUSIC", f"Spotify playlist : {len(tracks)} tracks")
            return tracks
        except Exception as e:
            log("MUSIC", f"Erreur Spotify : {e} — fallback YT")

    if not YT_DLP_OK:
        log("MUSIC", "yt_dlp non disponible — impossible de résoudre")
        return []

    loop = asyncio.get_running_loop()   # FIX : get_running_loop() pas get_event_loop()

    # ── Playlist YouTube ──────────────────────────────────────────
    is_yt_playlist = "list=" in query and ("youtube.com" in query or "youtu.be" in query)
    if is_yt_playlist:
        def _extract_playlist():
            with youtube_dl.YoutubeDL(YTDL_FLAT_OPTS) as ydl:
                info = ydl.extract_info(query, download=False)
                if not info:
                    return []
                result = []
                for e in (info.get("entries") or []):
                    if not e:
                        continue
                    title = e.get("title") or "Titre inconnu"
                    vid_id = e.get("id") or ""
                    # Reconstruire une URL propre depuis l'id
                    webpage = e.get("url") or e.get("webpage_url") or ""
                    if vid_id and (not webpage.startswith("http")):
                        webpage = f"https://www.youtube.com/watch?v={vid_id}"
                    if not webpage:
                        continue
                    result.append({
                        "title":     title,
                        "url":       webpage,
                        "duration":  int(e.get("duration") or 0),
                        "thumbnail": e.get("thumbnail"),
                    })
                return result
        try:
            tracks = await loop.run_in_executor(None, _extract_playlist)
            log("MUSIC", f"Playlist YT : {len(tracks)} tracks")
            return tracks
        except Exception as e:
            log("MUSIC", f"Erreur playlist YT : {e}")
            return []

    # ── URL directe (YouTube ou autre) ───────────────────────────
    is_url = query.startswith(("http://", "https://"))
    if is_url:
        def _extract_url():
            # extract_flat=True ici est OK car on veut juste titre/durée/thumbnail
            # L'URL de stream sera résolue séparément par _get_stream_url
            with youtube_dl.YoutubeDL(YTDL_SEARCH_OPTS) as ydl:
                info = ydl.extract_info(query, download=False)
                if not info:
                    return []
                title     = info.get("title") or "Titre inconnu"
                duration  = int(info.get("duration") or 0)
                thumbnail = info.get("thumbnail")
                webpage   = info.get("webpage_url") or query
                return [{"title": title, "url": webpage,
                         "duration": duration, "thumbnail": thumbnail}]
        try:
            tracks = await loop.run_in_executor(None, _extract_url)
            if tracks:
                log("MUSIC", f"URL directe : '{tracks[0]['title']}'")
            else:
                log("MUSIC", "URL directe : aucun résultat")
            return tracks
        except Exception as e:
            log("MUSIC", f"Erreur URL directe : {e}")
            return []

    # ── Recherche texte libre ─────────────────────────────────────
    # ytsearch1 = 1 résultat, extract_flat=True → rapide
    # webpage_url est fiable sur une recherche ytsearch
    def _extract_search():
        with youtube_dl.YoutubeDL(YTDL_SEARCH_OPTS) as ydl:
            info = ydl.extract_info(f"ytsearch1:{query}", download=False)
            if not info:
                return []
            entries = info.get("entries") or []
            if not entries:
                return []
            e = entries[0]
            if not e:
                return []
            title   = e.get("title") or "Titre inconnu"
            vid_id  = e.get("id") or ""
            webpage = e.get("webpage_url") or e.get("url") or ""
            # Si extract_flat retourne juste l'id sans URL complète, on reconstruit
            if vid_id and not webpage.startswith("http"):
                webpage = f"https://www.youtube.com/watch?v={vid_id}"
            if not webpage:
                return []
            return [{
                "title":     title,
                "url":       webpage,
                "duration":  int(e.get("duration") or 0),
                "thumbnail": e.get("thumbnail"),
            }]
    try:
        tracks = await loop.run_in_executor(None, _extract_search)
        if tracks:
            log("MUSIC", f"Recherche texte : trouvé '{tracks[0]['title']}' ({tracks[0]['url']})")
        else:
            log("MUSIC", f"Recherche texte : aucun résultat pour '{query}'")
        return tracks
    except Exception as e:
        log("MUSIC", f"Erreur recherche texte : {e}")
        return []


# ── Résolution URL de stream CDN audio ───────────────────────────
async def _get_stream_url(webpage_url: str) -> str | None:
    """
    Résout une webpage_url YouTube en URL de stream CDN audio.

    FIX CRITIQUE : utilise YTDL_STREAM_OPTS avec extract_flat=False.
    Sans ça, yt_dlp retourne la webpage_url inchangée au lieu du CDN audio,
    et FFmpegPCMAudio reçoit une URL HTML → erreur → déconnexion immédiate.
    """
    if not YT_DLP_OK:
        return None

    log("MUSIC", f"Résolution stream pour : {webpage_url[:70]}")
    loop = asyncio.get_running_loop()   # FIX : get_running_loop()

    def _fetch():
        with youtube_dl.YoutubeDL(YTDL_STREAM_OPTS) as ydl:
            info = ydl.extract_info(webpage_url, download=False)
            if not info:
                return None
            # "url" = URL du stream audio CDN (googlevideo.com ou équivalent)
            stream = info.get("url")
            if not stream:
                # Fallback : chercher dans les formats le meilleur audio
                fmts = info.get("formats") or []
                # Préférer audio-only (vcodec=none)
                audio_only = [f for f in fmts
                              if f.get("acodec") not in (None, "none")
                              and f.get("vcodec") in (None, "none")]
                if audio_only:
                    # Prendre le meilleur bitrate
                    audio_only.sort(key=lambda f: f.get("abr") or 0, reverse=True)
                    stream = audio_only[0].get("url")
                elif fmts:
                    stream = fmts[-1].get("url")
            return stream

    try:
        url = await loop.run_in_executor(None, _fetch)
        if url:
            log("MUSIC", "Stream URL résolu ✔")
        else:
            log("MUSIC", "Stream URL introuvable — aucun format audio disponible")
        return url
    except Exception as e:
        log("MUSIC", f"Erreur _get_stream_url : {e}")
        return None


# ── Cas Spotify : recherche YT sur titre seul ─────────────────────
async def _resolve_spotify_track_url(title: str) -> str | None:
    """Pour les tracks Spotify (url=None), cherche l'URL YT correspondante."""
    if not YT_DLP_OK:
        return None
    loop = asyncio.get_running_loop()
    def _search():
        with youtube_dl.YoutubeDL(YTDL_SEARCH_OPTS) as ydl:
            info = ydl.extract_info(f"ytsearch1:{title}", download=False)
            if not info:
                return None
            entries = info.get("entries") or []
            if not entries:
                return None
            e = entries[0]
            vid_id  = e.get("id") or ""
            webpage = e.get("webpage_url") or e.get("url") or ""
            if vid_id and not webpage.startswith("http"):
                webpage = f"https://www.youtube.com/watch?v={vid_id}"
            return webpage or None
    try:
        return await loop.run_in_executor(None, _search)
    except Exception as e:
        log("MUSIC", f"Erreur recherche YT pour Spotify track '{title}' : {e}")
        return None


# ── Utilitaires ───────────────────────────────────────────────────
def _fmt_duration(seconds: int) -> str:
    if not seconds:
        return "?"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


# ── Player ────────────────────────────────────────────────────────
async def _play_next(guild: discord.Guild):
    """
    Lance le prochain morceau de la file.
    Si la file est vide, attend 60 s d'inactivité puis déconnecte.
    """
    st = _mstate(guild.id)
    vc = guild.voice_client

    if not vc or not vc.is_connected():
        log("MUSIC", f"[{guild.name}] _play_next : vc déconnecté — abandon")
        st["current"] = None
        return

    if not st["queue"]:
        log("MUSIC", f"[{guild.name}] File vide — déconnexion dans 60 s si inactivité")
        st["current"] = None
        # Attendre 60 s avant de déconnecter (pas 2 s — évite les décos trop rapides)
        await asyncio.sleep(60)
        vc2 = guild.voice_client
        if vc2 and vc2.is_connected() and not vc2.is_playing() and not vc2.is_paused():
            log("MUSIC", f"[{guild.name}] Déconnexion après inactivité")
            await vc2.disconnect()
        return

    track = st["queue"].pop(0)
    st["current"] = track
    log("MUSIC", f"[{guild.name}] Lecture : '{track['title']}'")

    # ── Résoudre l'URL de stream ──────────────────────────────────
    # Cas Spotify : track["url"] est None → rechercher d'abord sur YT
    if not track.get("url"):
        log("MUSIC", f"[{guild.name}] Track Spotify sans URL — recherche YT")
        yt_url = await _resolve_spotify_track_url(track["title"])
        if not yt_url:
            log("MUSIC", f"[{guild.name}] Recherche YT échouée pour '{track['title']}' — skip")
            await _play_next(guild)
            return
        track["url"] = yt_url

    stream_url = track.get("stream_url")
    if not stream_url:
        stream_url = await _get_stream_url(track["url"])

    if not stream_url:
        log("MUSIC", f"[{guild.name}] Stream URL introuvable pour '{track['title']}' — skip")
        await _play_next(guild)
        return

    track["stream_url"] = stream_url

    # ── Créer la source audio ──────────────────────────────────────
    volume = st.get("volume", 0.5)
    try:
        source = discord.PCMVolumeTransformer(
            discord.FFmpegPCMAudio(stream_url, **FFMPEG_OPTS),
            volume=volume)
    except Exception as e:
        log("MUSIC", f"[{guild.name}] Erreur création FFmpegPCMAudio : {e}")
        await _play_next(guild)
        return

    # ── Callback after : déclenché par discord.py dans un thread séparé ──
    def _after(error):
        if error:
            log("MUSIC", f"[{guild.name}] Erreur lecture (after callback) : {error}")
        else:
            log("MUSIC", f"[{guild.name}] Fin de '{track['title']}'")
        # Planifier _play_next sur la boucle asyncio principale
        asyncio.run_coroutine_threadsafe(_play_next(guild), client.loop)

    # ── Lancer la lecture ──────────────────────────────────────────
    try:
        vc.play(source, after=_after)
        log("MUSIC", f"[{guild.name}] vc.play() lancé ✔")
    except discord.errors.ClientException as e:
        log("MUSIC", f"[{guild.name}] vc.play ClientException : {e}")
        await _play_next(guild)
        return
    except Exception as e:
        log("MUSIC", f"[{guild.name}] vc.play erreur inattendue : {e}")
        await _play_next(guild)
        return

    # ── Embed Now Playing ─────────────────────────────────────────
    ch_id = st.get("np_channel")
    ch    = guild.get_channel(ch_id) if ch_id else None
    if ch:
        try:
            await ch.send(embed=_now_playing_embed(track, guild))
        except Exception as e:
            log("MUSIC", f"[{guild.name}] Erreur envoi embed NP : {e}")


def _now_playing_embed(track: dict, guild: discord.Guild) -> discord.Embed:
    e = discord.Embed(
        title="🎵  Lecture en cours",
        description=f"**{track['title']}**",
        color=0x1DB954,
        timestamp=datetime.now(timezone.utc),
    )
    if track.get("url"):
        e.description = f"**[{track['title']}]({track['url']})**"
    if track.get("thumbnail"):
        e.set_thumbnail(url=track["thumbnail"])
    e.add_field(name="⏱  Durée",        value=_fmt_duration(track.get("duration", 0)), inline=True)
    e.add_field(name="👤  Demandé par", value=track.get("requester", "?"),              inline=True)
    vc = guild.voice_client
    if vc and vc.channel:
        e.add_field(name="🔊  Vocal", value=vc.channel.name, inline=True)
    n = len(_mstate(guild.id)["queue"])
    if n:
        e.set_footer(text=f"{n} morceau(x) suivant(s) dans la file")
    return e


# ── Autocomplete /play ────────────────────────────────────────────
async def _autocomplete_play(inter: discord.Interaction, current: str):
    """
    Retourne jusqu'à 5 suggestions YouTube pendant que l'utilisateur tape.
    FIX : utilise YTDL_SEARCH_OPTS dédié (extract_flat=True isolé)
    au lieu de copier YTDL_OPTS qui contenait extract_flat='in_playlist'.
    """
    if not current or len(current) < 2:
        return []
    if not YT_DLP_OK:
        return []

    loop = asyncio.get_running_loop()   # FIX : get_running_loop()

    def _search():
        opts = dict(YTDL_SEARCH_OPTS)
        opts["playlistend"] = 5
        with youtube_dl.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(f"ytsearch5:{current}", download=False)
            if not info:
                return []
            results = []
            for e in (info.get("entries") or []):
                if not e:
                    continue
                title   = e.get("title") or "?"
                vid_id  = e.get("id") or ""
                webpage = e.get("webpage_url") or e.get("url") or ""
                if vid_id and not webpage.startswith("http"):
                    webpage = f"https://www.youtube.com/watch?v={vid_id}"
                if webpage:
                    results.append((title, webpage))
            return results

    try:
        results = await loop.run_in_executor(None, _search)
        return [
            app_commands.Choice(name=t[:100], value=u)
            for t, u in results if u
        ][:5]
    except Exception as e:
        log("MUSIC", f"Autocomplete erreur : {e}")
        return []

async def handle_cmds(message):
    if not message.guild: return
    content = message.content.strip()
    guild   = message.guild
    cfg     = tconf(guild.id)

    # Les actions sur ticket se font uniquement via le salon staff (boutons/menu) ou commandes !taccept/trefuse/tclose
    # On ignore !accept/refuse/close tapés dans le salon ticket lui-même

    if not content.startswith("!"): return
    parts = content[1:].split(); cmd = parts[0].lower(); args = parts[1:]

    if cmd == "ticket":
        if maintenance_mode:
            await message.channel.send("🔧 **Bot en maintenance** — tickets temporairement indisponibles."); return
        ok_cfg, msg_cfg = is_configured(guild.id)
        if not ok_cfg: await message.channel.send("❌ " + msg_cfg); return
        if not args:
            if not cfg["types"]: await message.channel.send("❌ Aucun type de ticket configuré."); return
            await message.channel.send("🎫 **Ouvrir un ticket :**\n" +
                "\n".join("• `!ticket " + k + "` — " + v["emoji"] + " " + v["label"] for k, v in cfg["types"].items()))
            return
        key = args[0].lower()
        if key not in cfg["types"]: await message.channel.send("❌ Type inconnu : `" + key + "`"); return
        ch, err = await open_ticket(guild, message.author, key)
        if err: await message.channel.send("❌ " + err)
        return

    if not is_staff(message.author, guild.id): return

    if cmd == "tpending":
        gp = [(k, v) for k, v in pending_channels.items() if v["guild_id"] == guild.id]
        if not gp: await message.channel.send("📭 Aucun salon en attente."); return
        lines = ["`" + str(i) + ".` <#" + str(k) + ">  " + v["detected_at"] for i, (k, v) in enumerate(gp, 1)]
        await message.channel.send("📥 **En attente :**\n" + "\n".join(lines) + "\n`!tlaunch <n°> <type>`")

    elif cmd == "tlaunch":
        if len(args) < 2: await message.channel.send("❌ `!tlaunch <n°> <type> [membre]`"); return
        gp = [(k, v) for k, v in pending_channels.items() if v["guild_id"] == guild.id]
        ch_obj = None; pdata = None
        try:
            idx = int(args[0]) - 1
            if 0 <= idx < len(gp): ch_obj = guild.get_channel(gp[idx][0]); pdata = gp[idx][1]
        except ValueError:
            for k, v in gp:
                c = guild.get_channel(k)
                if c and args[0].lstrip("#").lower() in c.name.lower(): ch_obj = c; pdata = v; break
        if not ch_obj: await message.channel.send("❌ Salon introuvable."); return
        key = args[1].lower()
        if key not in cfg["types"]: await message.channel.send("❌ Types : " + ", ".join("`" + k + "`" for k in cfg["types"])); return
        # Priorité : 3e arg → owner_id stocké → premier non-admin dans salon
        member = None
        if len(args) >= 3:
            member = find_member(args[2], guild)
            if not member: await message.channel.send("❌ Membre introuvable."); return
        if not member:
            owner_id = (pdata or {}).get("owner_id")
            if owner_id: member = guild.get_member(owner_id)
        if not member:
            membres_ch = [m for m in guild.members if not m.bot and ch_obj.permissions_for(m).view_channel
                          and not m.guild_permissions.administrator]
            if not membres_ch: membres_ch = [m for m in guild.members if not m.bot and ch_obj.permissions_for(m).view_channel]
            if not membres_ch: await message.channel.send("❌ Aucun membre dans ce salon."); return
            member = membres_ch[0]
        await message.channel.send("▶️ Lancement pour " + member.mention + " dans " + ch_obj.mention + "…")
        asyncio.create_task(run_questions(guild, cfg, ch_obj, member, key))

    elif cmd == "tlist":
        filtre = args[0].lower() if args else "open"
        ss = [s for s in ticket_sessions.values() if s["guild_id"] == guild.id]
        if filtre == "open":   ss = [s for s in ss if s["status"] in ("open", "pending")]
        elif filtre == "closed": ss = [s for s in ss if s["status"] not in ("open", "pending")]
        if not ss: await message.channel.send("📭 Aucun ticket (" + filtre + ")."); return
        icons = {"open": "🟡", "pending": "🟡", "accepted": "✅", "refused": "❌", "closed": "🔒"}
        await message.channel.send("🎫 **Tickets :**\n" + "\n".join(
            icons.get(s["status"], "❓") + " `#" + str(s["number"]).zfill(4) + "` " + s["type"] + " <@" + str(s["user_id"]) + ">"
            for s in sorted(ss, key=lambda x: x["number"])))

    elif cmd in ("taccept", "trefuse", "tclose"):
        if not args: await message.channel.send("❌ `!" + cmd + " <n°>`"); return
        try: num = int(args[0].lstrip("#"))
        except ValueError: await message.channel.send("❌ Numéro invalide."); return
        s = next((x for x in ticket_sessions.values() if x["guild_id"] == guild.id and x["number"] == num), None)
        if not s: await message.channel.send("❌ Ticket #" + str(num) + " introuvable."); return
        act = {"taccept": "accepted", "trefuse": "refused", "tclose": "closed"}[cmd]
        raison = " ".join(args[1:]) if len(args) > 1 else {"accepted": "Accepté.", "refused": "Refusé.", "closed": "Fermé."}.get(act, "")
        await do_action(guild, s, act, raison, message.author)
        await message.add_reaction("✅")

    elif cmd == "tsend":
        if len(args) < 2: await message.channel.send("❌ `!tsend <n°> <msg>`"); return
        try: num = int(args[0].lstrip("#"))
        except ValueError: await message.channel.send("❌ Numéro invalide."); return
        s = next((x for x in ticket_sessions.values() if x["guild_id"] == guild.id and x["number"] == num), None)
        if not s: await message.channel.send("❌ Ticket #" + str(num) + " introuvable."); return
        ch_obj = guild.get_channel(s["channel_id"])
        if not ch_obj: await message.channel.send("❌ Salon introuvable."); return
        await ch_obj.send("📢 **Staff** (" + message.author.display_name + ") : " + " ".join(args[1:]))
        await message.add_reaction("✅")

    elif cmd == "thelp":
        await message.channel.send(
            "🎫 **Commandes tickets**\n"
            "`!ticket [type]` `!tpending` `!tlaunch <n°> <type>`\n"
            "`!tlist [open|closed|all]` `!taccept <n°>` `!trefuse <n°>` `!tclose <n°>`\n"
            "`!tsend <n°> <msg>` `/menu`")



# ════════════════════════════════════════════════════════════════
#  SLASH COMMANDS
# ════════════════════════════════════════════════════════════════
@tree.command(name="menu", description="Ouvrir le panel de gestion staff")
async def slash_menu(inter: discord.Interaction):
    try:
        # Vérifications rapides (< 1 ms) — répondent avant tout traitement lent
        if not inter.guild:
            await inter.response.send_message(
                "Cette commande n'est disponible que sur un serveur.", ephemeral=True)
            return
        guild = inter.guild
        cfg   = tconf(guild.id)
        if maintenance_mode:
            await inter.response.send_message(
                "🔧 Le bot est actuellement en maintenance. Réessaie plus tard.", ephemeral=True)
            return
        if cfg.get("menu_locked"):
            await inter.response.send_message(
                "🔒 Le menu est actuellement désactivé par l'administration.", ephemeral=True)
            return
        if not is_staff(inter.user, guild.id):
            await inter.response.send_message("🚫 Accès réservé au staff.", ephemeral=True)
            return
        # defer() immédiat — Discord reçoit la réponse en < 1s, on a 15 min pour le followup
        await inter.response.defer(ephemeral=False)
        await send_staff_menu_inter(inter, guild)
    except discord.errors.InteractionResponded:
        # L'interaction a déjà été répondue (cas de double-clic rapide)
        pass
    except Exception as e:
        import traceback
        log("SLASH", f"/menu exception : {e}")
        log("SLASH", traceback.format_exc())
        # Tenter de notifier l'utilisateur si possible
        try:
            msg = "❌ Une erreur interne s'est produite. Réessaie dans quelques secondes."
            if inter.response.is_done():
                await inter.followup.send(msg, ephemeral=True)
            else:
                await inter.response.send_message(msg, ephemeral=True)
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════
#  SLASH COMMANDS MUSIQUE
# ══════════════════════════════════════════════════════════════════

def _music_check(inter: discord.Interaction) -> tuple[bool, str]:
    """Vérifie : guild OK, vocal OK, accès OK. Retourne (ok, error_msg)."""
    if not inter.guild:
        return False, "Cette commande n'est disponible que sur un serveur."
    ok_, msg = _can_use_music(inter.user, inter.guild.id)
    if not ok_:
        return False, msg
    return True, ""

@tree.command(name="play", description="Lancer de la musique (titre, lien ou playlist Spotify)")
@app_commands.describe(query="Titre, lien YouTube ou lien de playlist Spotify")
@app_commands.autocomplete(query=_autocomplete_play)
async def slash_play(inter: discord.Interaction, query: str):
    try:
        ok_, msg = _music_check(inter)
        if not ok_:
            await inter.response.send_message(msg, ephemeral=True); return
        if not inter.user.voice or not inter.user.voice.channel:
            await inter.response.send_message("🔇 Tu dois être dans un salon vocal.", ephemeral=True); return
        if not YT_DLP_OK:
            await inter.response.send_message(
                "❌ `yt_dlp` n'est pas installé sur le serveur. Ajoute `yt_dlp` à `requirements.txt`.",
                ephemeral=True); return

        await inter.response.defer()

        # Résoudre la requête
        tracks = await _resolve_query(query)
        if not tracks:
            await inter.followup.send("❌ Aucun résultat pour cette recherche.", ephemeral=True); return

        # Renseigner le demandeur
        for t in tracks:
            t["requester"] = inter.user.display_name

        guild = inter.guild
        st    = _mstate(guild.id)
        st["np_channel"] = inter.channel_id

        # Rejoindre le vocal si pas connecté
        vc = guild.voice_client
        target_channel = inter.user.voice.channel
        if vc and vc.is_connected():
            if vc.channel != target_channel:
                await vc.move_to(target_channel)
        else:
            try:
                vc = await target_channel.connect()
            except Exception as e:
                await inter.followup.send(f"❌ Impossible de rejoindre le salon vocal : {e}"); return

        # Synchroniser le volume
        vol_pct = tconf(guild.id).get("music_volume", 50)
        st["volume"] = vol_pct / 100

        # Ajouter les tracks à la file
        was_empty = not st["queue"] and st["current"] is None
        st["queue"].extend(tracks)

        if len(tracks) == 1:
            title = tracks[0]["title"]
            msg   = f"✅ **{title}** ajouté à la file."
        else:
            msg = f"✅ **{len(tracks)} morceaux** ajoutés à la file."

        await inter.followup.send(msg)

        # Lancer la lecture si rien ne joue
        if was_empty and not (vc.is_playing() or vc.is_paused()):
            await _play_next(guild)

    except discord.errors.InteractionResponded:
        pass
    except Exception as e:
        import traceback
        log("SLASH", f"/play exception : {e}"); log("SLASH", traceback.format_exc())
        try:
            m = "❌ Erreur lors de la lecture. Réessaie."
            if inter.response.is_done(): await inter.followup.send(m, ephemeral=True)
            else: await inter.response.send_message(m, ephemeral=True)
        except Exception: pass


@tree.command(name="stop", description="Arrêter la musique et déconnecter le bot du vocal")
async def slash_stop(inter: discord.Interaction):
    try:
        ok_, msg = _music_check(inter)
        if not ok_:
            await inter.response.send_message(msg, ephemeral=True); return
        guild = inter.guild
        vc    = guild.voice_client if guild else None
        if not vc or not vc.is_connected():
            await inter.response.send_message("🔇 Le bot n'est pas dans un salon vocal.", ephemeral=True); return
        st = _mstate(guild.id)
        st["queue"].clear()
        st["current"] = None
        await vc.disconnect()
        await inter.response.send_message("⏹  Musique arrêtée et bot déconnecté.")
    except Exception as e:
        log("SLASH", f"/stop : {e}")
        try: await inter.response.send_message("❌ Erreur.", ephemeral=True)
        except Exception: pass


@tree.command(name="skip", description="Passer au morceau suivant")
async def slash_skip(inter: discord.Interaction):
    try:
        ok_, msg = _music_check(inter)
        if not ok_:
            await inter.response.send_message(msg, ephemeral=True); return
        guild = inter.guild
        vc    = guild.voice_client if guild else None
        if not vc or not vc.is_playing():
            await inter.response.send_message("🔇 Rien n'est en lecture.", ephemeral=True); return
        st = _mstate(guild.id)
        cur = st.get("current")
        title = cur["title"] if cur else "?"
        vc.stop()   # déclenche _play_next via after callback
        n = len(st["queue"])
        msg = f"⏭  **{title}** skippé."
        if n: msg += f" {n} morceau(x) restant(s)."
        else: msg += " File vide."
        await inter.response.send_message(msg)
    except Exception as e:
        log("SLASH", f"/skip : {e}")
        try: await inter.response.send_message("❌ Erreur.", ephemeral=True)
        except Exception: pass


@tree.command(name="pause", description="Mettre la musique en pause")
async def slash_pause(inter: discord.Interaction):
    try:
        ok_, msg = _music_check(inter)
        if not ok_:
            await inter.response.send_message(msg, ephemeral=True); return
        guild = inter.guild
        vc    = guild.voice_client if guild else None
        if not vc or not vc.is_playing():
            await inter.response.send_message("🔇 Rien n'est en lecture.", ephemeral=True); return
        vc.pause()
        await inter.response.send_message("⏸  Lecture mise en pause.")
    except Exception as e:
        log("SLASH", f"/pause : {e}")
        try: await inter.response.send_message("❌ Erreur.", ephemeral=True)
        except Exception: pass


@tree.command(name="resume", description="Reprendre la lecture")
async def slash_resume(inter: discord.Interaction):
    try:
        ok_, msg = _music_check(inter)
        if not ok_:
            await inter.response.send_message(msg, ephemeral=True); return
        guild = inter.guild
        vc    = guild.voice_client if guild else None
        if not vc or not vc.is_paused():
            await inter.response.send_message("▶️  La lecture n'est pas en pause.", ephemeral=True); return
        vc.resume()
        await inter.response.send_message("▶️  Lecture reprise.")
    except Exception as e:
        log("SLASH", f"/resume : {e}")
        try: await inter.response.send_message("❌ Erreur.", ephemeral=True)
        except Exception: pass


@tree.command(name="queue", description="Afficher la file d'attente")
async def slash_queue(inter: discord.Interaction):
    try:
        if not inter.guild:
            await inter.response.send_message("Commande disponible sur un serveur uniquement.", ephemeral=True); return
        await inter.response.defer(ephemeral=True)
        st  = _mstate(inter.guild.id)
        cur = st.get("current")
        q   = st.get("queue", [])
        vc  = inter.guild.voice_client

        e = discord.Embed(title="🎶  File d'attente", color=0x1DB954,
                          timestamp=datetime.now(timezone.utc))

        if cur:
            status = "▶️  En lecture" if (vc and vc.is_playing()) else "⏸  En pause"
            e.add_field(
                name=f"{status}",
                value="**" + cur["title"] + "**\n⏱ " + _fmt_duration(cur.get("duration",0)) + "  •  👤 " + cur.get("requester","?"),
                inline=False)
        else:
            e.add_field(name="Aucune lecture en cours", value="File vide.", inline=False)

        if q:
            lines = []
            for i, t in enumerate(q[:10], 1):
                lines.append(f"`{i}.` {t['title']}  `{_fmt_duration(t.get('duration',0))}`")
            if len(q) > 10:
                lines.append(f"… et {len(q)-10} autre(s)")
            e.add_field(name=f"📋  Suivants ({len(q)})", value="\n".join(lines), inline=False)

        await inter.followup.send(embed=e, ephemeral=True)
    except Exception as e:
        log("SLASH", f"/queue : {e}")
        try:
            if inter.response.is_done(): await inter.followup.send("❌ Erreur.", ephemeral=True)
            else: await inter.response.send_message("❌ Erreur.", ephemeral=True)
        except Exception: pass


@tree.command(name="volume", description="Régler le volume (0–100)")
@app_commands.describe(level="Volume de 0 à 100")
async def slash_volume(inter: discord.Interaction, level: int):
    try:
        ok_, msg = _music_check(inter)
        if not ok_:
            await inter.response.send_message(msg, ephemeral=True); return
        if not 0 <= level <= 100:
            await inter.response.send_message("❌ Le volume doit être entre 0 et 100.", ephemeral=True); return
        guild = inter.guild
        st    = _mstate(guild.id)
        st["volume"] = level / 100
        # Appliquer immédiatement si en cours de lecture
        vc = guild.voice_client
        if vc and vc.source and isinstance(vc.source, discord.PCMVolumeTransformer):
            vc.source.volume = level / 100
        # Sauvegarder dans la config serveur
        tconf(guild.id)["music_volume"] = level
        await save_data()
        bar = "█" * (level // 10) + "░" * (10 - level // 10)
        await inter.response.send_message(f"🔊  Volume : `{bar}` **{level}%**")
    except Exception as e:
        log("SLASH", f"/volume : {e}")
        try: await inter.response.send_message("❌ Erreur.", ephemeral=True)
        except Exception: pass

# ══════════════════════════════════════════════════════════════════
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


async def send_staff_menu(channel, guild, invoker=None):
    """Envoie le panel staff. Supprime l'ancien message actif pour ce serveur."""
    if invoker and not is_staff(invoker, guild.id):
        await channel.send(embed=_e_err("🚫  Accès refusé", "Réservé au staff configuré."))
        return
    cfg = tconf(guild.id)
    if cfg.get("menu_locked"):
        if invoker:
            await channel.send(embed=_e_warn("🔒  Menu verrouillé [" + guild.name + "]", "Déverrouille via la console (`lockmenu off`)."))
        return
    # Supprimer l'ancien menu actif pour ce serveur (via ID mémorisé ou scan du salon)
    await _delete_active_menu(guild.id, channel=channel)

    cat = guild.get_channel(cfg.get("category_id") or 0)
    sch = guild.get_channel(cfg.get("staff_channel_id") or 0)
    sro = guild.get_role(cfg.get("staff_role_id") or 0)

    ot = [s for s in ticket_sessions.values() if s["guild_id"] == guild.id and s["status"] in ("open", "pending")]
    pt = [v for v in pending_channels.values() if v["guild_id"] == guild.id]
    ct = [s for s in ticket_sessions.values() if s["guild_id"] == guild.id and s["status"] in ("accepted", "refused", "closed")]

    def bar(n, total, w=10):
        if total == 0: return "`" + "░" * w + "`"
        f = round(n / total * w)
        return "`" + "█" * f + "░" * (w - f) + "`"

    total_t = max(len(ot) + len(ct), 1)

    e = discord.Embed(color=C_DARK, timestamp=datetime.now(timezone.utc))
    e.set_author(name="⚡  Panel Staff — " + guild.name,
                 icon_url=guild.icon.url if guild.icon else None)

    # Config bloc
    cfg_val  = "> 📁  **Catégorie** — " + ("`" + cat.name + "`" if cat else "❌ Non définie") + "\n"
    cfg_val += "> 📣  **Salon staff** — " + (sch.mention if sch else "❌ Non défini") + "\n"
    cfg_val += "> 🛡  **Rôle staff** — " + (sro.mention if sro else "⚠️ Non défini")
    e.add_field(name="⚙️  Configuration", value=cfg_val, inline=False)

    # Stats
    stats_val  = "> 🟡  **En cours**    " + bar(len(ot), total_t) + "  `" + str(len(ot)) + "`\n"
    stats_val += "> ✅  **Traités**     " + bar(len(ct), total_t) + "  `" + str(len(ct)) + "`\n"
    stats_val += "> 📥  **En attente** (DraftBot)  `" + str(len(pt)) + "`\n"
    stats_val += "> 📝  **Types configurés**        `" + str(len(cfg["types"])) + "`"
    e.add_field(name="📊  Statistiques", value=stats_val, inline=False)

    # Derniers tickets
    if ot:
        icons = {"open": "🟡", "pending": "⏳"}
        recent = sorted(ot, key=lambda x: x["number"], reverse=True)[:3]
        last = ""
        for s in recent:
            mem  = guild.get_member(s["user_id"])
            name = mem.display_name if mem else str(s["user_id"])
            last += "> " + icons.get(s["status"], "🟡") + "  `#" + str(s["number"]).zfill(4) + "` **" + s["type"] + "** — " + name + "\n"
        e.add_field(name="🕐  Tickets récents", value=last.rstrip(), inline=False)

    e.set_footer(text=("🔧 MAINTENANCE  •  " if maintenance_mode else "") +
                 ("🔒 Menu verrouillé  •  " if cfg.get("menu_locked") else "") +
                 "Panel staff  •  " + str(len([m for m in guild.members if not m.bot])) + " membres")
    msg = await channel.send(embed=e, view=DashboardView(guild))
    active_menu_msgs[guild.id] = (channel.id, msg.id)


async def send_staff_menu_inter(inter: discord.Interaction, guild):
    """Version de send_staff_menu qui répond à une interaction slash."""
    cfg = tconf(guild.id)
    # Supprimer l'ancien menu actif
    await _delete_active_menu(guild.id, channel=inter.channel)

    cat = guild.get_channel(cfg.get("category_id") or 0)
    sch = guild.get_channel(cfg.get("staff_channel_id") or 0)
    sro = guild.get_role(cfg.get("staff_role_id") or 0)
    ot  = [s for s in ticket_sessions.values() if s["guild_id"] == guild.id and s["status"] in ("open", "pending")]
    pt  = [v for v in pending_channels.values() if v["guild_id"] == guild.id]
    ct  = [s for s in ticket_sessions.values() if s["guild_id"] == guild.id and s["status"] in ("accepted", "refused", "closed")]

    def bar(n, total, w=10):
        if total == 0: return "`" + "░" * w + "`"
        f = round(n / total * w)
        return "`" + "█" * f + "░" * (w - f) + "`"

    total_t = max(len(ot) + len(ct), 1)
    e = discord.Embed(color=C_DARK, timestamp=datetime.now(timezone.utc))
    e.set_author(name="⚡  Panel Staff — " + guild.name,
                 icon_url=guild.icon.url if guild.icon else None)

    cfg_val  = "> 📁  **Catégorie** — " + ("`" + cat.name + "`" if cat else "❌ Non définie") + "\n"
    cfg_val += "> 📣  **Salon staff** — " + (sch.mention if sch else "❌ Non défini") + "\n"
    cfg_val += "> 🛡  **Rôle staff** — " + (sro.mention if sro else "⚠️ Non défini")
    e.add_field(name="⚙️  Configuration", value=cfg_val, inline=False)

    stats_val  = "> 🟡  **En cours**    " + bar(len(ot), total_t) + "  `" + str(len(ot)) + "`\n"
    stats_val += "> ✅  **Traités**     " + bar(len(ct), total_t) + "  `" + str(len(ct)) + "`\n"
    stats_val += "> 📥  **En attente** (DraftBot)  `" + str(len(pt)) + "`\n"
    stats_val += "> 📝  **Types configurés**        `" + str(len(cfg["types"])) + "`"
    e.add_field(name="📊  Statistiques", value=stats_val, inline=False)

    if ot:
        icons = {"open": "🟡", "pending": "⏳"}
        recent = sorted(ot, key=lambda x: x["number"], reverse=True)[:3]
        last = ""
        for s in recent:
            mem  = guild.get_member(s["user_id"])
            name = mem.display_name if mem else str(s["user_id"])
            last += "> " + icons.get(s["status"], "🟡") + "  `#" + str(s["number"]).zfill(4) + "` **" + s["type"] + "** — " + name + "\n"
        e.add_field(name="🕐  Tickets récents", value=last.rstrip(), inline=False)

    e.set_footer(text=("🔧 MAINTENANCE  •  " if maintenance_mode else "") +
                 ("🔒 Menu verrouillé  •  " if cfg.get("menu_locked") else "") +
                 "Panel staff  •  " + str(len([m for m in guild.members if not m.bot])) + " membres")

    # Envoyer le panel — via followup si defer() a déjà été appelé, sinon send_message
    if inter.response.is_done():
        msg = await inter.followup.send(embed=e, view=DashboardView(guild), wait=True)
    else:
        await inter.response.send_message(embed=e, view=DashboardView(guild))
        msg = await inter.original_response()
    active_menu_msgs[guild.id] = (inter.channel.id, msg.id)


class DashboardView(discord.ui.View):
    def __init__(self, guild):
        super().__init__(timeout=600)
        self.guild = guild
        sel = discord.ui.Select(
            placeholder="📂  Choisir une section…",
            row=0,
            options=[
                discord.SelectOption(label="🎫  Tickets ouverts",    value="tickets",  description="Voir et gérer les tickets en cours"),
                discord.SelectOption(label="📥  Salons en attente",   value="pending",  description="Lancer les questions DraftBot"),
                discord.SelectOption(label="📝  Types de tickets",    value="types",    description="Voir, créer, modifier, supprimer les types"),
                discord.SelectOption(label="⚙️   Configuration",      value="config",   description="Catégorie, salon staff, rôle staff"),
                discord.SelectOption(label="👥  Membres",             value="members",  description="Liste des membres et modération"),
                discord.SelectOption(label="📋  Rôles",               value="roles",    description="Liste des rôles du serveur"),
                discord.SelectOption(label="📡  Salons",              value="channels", description="Salons texte par catégorie"),
                discord.SelectOption(label="🌐  Infos serveur",       value="server",   description="Statistiques du serveur"),
            ]
        )
        sel.callback = self.on_select
        self.add_item(sel)
        # Bouton Fermer le menu
        btn_close = discord.ui.Button(label="Fermer le menu", style=discord.ButtonStyle.secondary,
                                      emoji="✖️", row=1)
        btn_close.callback = self.close_menu
        self.add_item(btn_close)
        # Bouton Rafraîchir
        btn_refresh = discord.ui.Button(label="Rafraîchir", style=discord.ButtonStyle.secondary,
                                        emoji="🔄", row=1)
        btn_refresh.callback = self.refresh_menu
        self.add_item(btn_refresh)

    def _guard(self, inter):
        cfg = tconf(self.guild.id)
        if maintenance_mode: return False, "🔧 Bot en maintenance."
        if not is_staff(inter.user, self.guild.id): return False, "🚫 Réservé au staff."
        if cfg.get("menu_locked"): return False, "🔒 Menu verrouillé sur ce serveur."
        return True, None

    async def close_menu(self, inter: discord.Interaction):
        try: await inter.message.delete()
        except: pass
        active_menu_msgs.pop(self.guild.id, None)
        await inter.response.send_message(embed=_e_empty("✖️  Menu fermé"), ephemeral=True)
        self.stop()

    async def refresh_menu(self, inter: discord.Interaction):
        ok2, reason = self._guard(inter)
        if not ok2:
            await inter.response.send_message(embed=_e_warn("⚠️", reason), ephemeral=True); return
        # Reconstruire l'embed à jour et éditer le message EN PLACE (pas de nouveau message)
        cfg = tconf(self.guild.id)
        cat = self.guild.get_channel(cfg.get("category_id") or 0)
        sch = self.guild.get_channel(cfg.get("staff_channel_id") or 0)
        sro = self.guild.get_role(cfg.get("staff_role_id") or 0)
        ot  = [s for s in ticket_sessions.values() if s["guild_id"] == self.guild.id and s["status"] in ("open", "pending")]
        pt  = [v for v in pending_channels.values() if v["guild_id"] == self.guild.id]
        ct  = [s for s in ticket_sessions.values() if s["guild_id"] == self.guild.id and s["status"] in ("accepted", "refused", "closed")]
        total_t = max(len(ot) + len(ct), 1)
        def bar(n, t, w=10):
            if t == 0: return "`" + "░" * w + "`"
            f = round(n / t * w); return "`" + "█" * f + "░" * (w - f) + "`"
        e = discord.Embed(color=C_DARK, timestamp=datetime.now(timezone.utc))
        e.set_author(name="⚡  Panel Staff — " + self.guild.name,
                     icon_url=self.guild.icon.url if self.guild.icon else None)
        cfg_val  = "> 📁  **Catégorie** — " + ("`" + cat.name + "`" if cat else "❌ Non définie") + "\n"
        cfg_val += "> 📣  **Salon staff** — " + (sch.mention if sch else "❌ Non défini") + "\n"
        cfg_val += "> 🛡  **Rôle staff** — " + (sro.mention if sro else "⚠️ Non défini")
        e.add_field(name="⚙️  Configuration", value=cfg_val, inline=False)
        stats_val  = "> 🟡 En cours " + bar(len(ot), total_t) + " `" + str(len(ot)) + "`\n"
        stats_val += "> ✅ Traités   " + bar(len(ct), total_t) + " `" + str(len(ct)) + "`\n"
        stats_val += "> 📥 En attente `" + str(len(pt)) + "`  •  📝 Types `" + str(len(cfg["types"])) + "`"
        e.add_field(name="📊  Statistiques", value=stats_val, inline=False)
        e.set_footer(text=("🔧 MAINTENANCE  •  " if maintenance_mode else "") +
                     ("🔒 Menu verrouillé  •  " if cfg.get("menu_locked") else "") +
                     "Panel staff  •  " + str(len([m for m in self.guild.members if not m.bot])) + " membres  •  🔄 " + datetime.now().strftime("%H:%M:%S"))
        # Éditer le message existant — aucun nouveau message créé
        await inter.response.edit_message(embed=e, view=DashboardView(self.guild))
        active_menu_msgs[self.guild.id] = (inter.channel.id, inter.message.id)

    async def on_select(self, inter: discord.Interaction):
        ok2, reason = self._guard(inter)
        if not ok2:
            await inter.response.send_message(embed=_e_warn("⚠️", reason), ephemeral=True); return
        val = inter.data["values"][0]
        g   = self.guild
        cfg = tconf(g.id)

        # ── Tickets ouverts ──────────────────────────────────────
        if val == "tickets":
            ss = [s for s in ticket_sessions.values()
                  if s["guild_id"] == g.id and s["status"] in ("open", "pending")]
            if not ss:
                await inter.response.send_message(embed=_e_empty("🎫  Tickets ouverts", "Aucun ticket en cours."), ephemeral=True)
                return
            icons = {"open": "🟡", "pending": "⏳"}
            e = discord.Embed(title="🎫  Tickets en cours",
                              description="`" + str(len(ss)) + "` ticket(s) ouvert(s)",
                              color=C_ORANGE)
            for s in sorted(ss, key=lambda x: x["number"])[:9]:
                mem = g.get_member(s["user_id"])
                mn  = mem.display_name if mem else str(s["user_id"])
                ico = icons.get(s["status"], "🟡")
                tt  = cfg["types"].get(s["type"], {})
                field_val = "👤 **" + mn + "**\n📅 " + s["opened_at"] + "\n<#" + str(s["channel_id"]) + ">"
                e.add_field(name=ico + "  #" + str(s["number"]).zfill(4) + " · " + tt.get("emoji", "🎫") + " " + tt.get("label", s["type"]),
                            value=field_val, inline=True)
            await inter.response.send_message(embed=e, view=TicketListView(g, ss, cfg), ephemeral=True)

        # ── Salons en attente ────────────────────────────────────
        elif val == "pending":
            gp = [(k, v) for k, v in pending_channels.items() if v["guild_id"] == g.id]
            if not gp:
                await inter.response.send_message(embed=_e_empty("📥  Salons en attente", "Aucun salon détecté."), ephemeral=True)
                return
            ok_cfg2, msg_cfg2 = is_configured(g.id)
            if not ok_cfg2:
                await inter.response.send_message(embed=_e_warn("⚠️  Config incomplète", msg_cfg2), ephemeral=True); return
            if not cfg["types"]:
                await inter.response.send_message(embed=_e_warn("⚠️  Aucun type défini", "Crée des types via la section Types."), ephemeral=True)
                return
            e = discord.Embed(title="📥  Salons en attente",
                              description="`" + str(len(gp)) + "` salon(s) détecté(s) par DraftBot",
                              color=C_AMBER)
            for k, v in gp[:10]:
                ch = g.get_channel(k)
                e.add_field(name="#" + (ch.name if ch else str(k)), value="⏱ " + v["detected_at"], inline=True)
            await inter.response.send_message(embed=e, view=PendingLaunchView(g, cfg, gp), ephemeral=True)

        # ── Types de tickets ─────────────────────────────────────
        elif val == "types":
            e = discord.Embed(title="📝  Types de tickets",
                              description="`" + str(len(cfg["types"])) + "` type(s) configuré(s)",
                              color=C_PURPLE)
            if cfg["types"]:
                for key, t in cfg["types"].items():
                    qs    = t.get("questions", [])
                    q_txt = "\n".join("• " + q for q in qs[:5]) or "*Aucune question*"
                    if len(qs) > 5: q_txt += "\n*…+" + str(len(qs) - 5) + " autres*"
                    e.add_field(name=t["emoji"] + "  **" + t["label"] + "**  `[" + key + "]`",
                                value=q_txt[:256], inline=False)
            else:
                e.description += "\n\n*Aucun type — crées-en un avec le bouton ci-dessous.*"
            await inter.response.send_message(embed=e, view=TypesManagerView(g, cfg), ephemeral=True)

        # ── Configuration ────────────────────────────────────────
        elif val == "config":
            cat2 = g.get_channel(cfg.get("category_id") or 0)
            sch2 = g.get_channel(cfg.get("staff_channel_id") or 0)
            sro2 = g.get_role(cfg.get("staff_role_id") or 0)
            e = discord.Embed(title="⚙️  Configuration du système de tickets", color=C_BLUE)
            e.add_field(name="📁  Catégorie tickets",
                        value="`" + cat2.name + "`" if cat2 else "❌ *Non définie*", inline=True)
            e.add_field(name="📣  Salon staff",
                        value=sch2.mention if sch2 else "❌ *Non défini*", inline=True)
            e.add_field(name="🛡  Rôle staff",
                        value=sro2.mention if sro2 else "⚠️ *Non défini*", inline=True)
            e.add_field(name="🔢  Compteur",
                        value="`#" + str(cfg["counter"]).zfill(4) + "`", inline=True)
            e.add_field(name="📝  Types",
                        value="`" + str(len(cfg["types"])) + "` type(s)", inline=True)
            e.add_field(name="🔒  Menu Discord",
                        value="Verrouillé 🔒" if cfg.get("menu_locked") else "Libre 🔓", inline=True)
            e.set_footer(text="Utilise les menus ci-dessous pour modifier la configuration.")
            await inter.response.send_message(embed=e, view=ConfigFullView(g, cfg), ephemeral=True)

        # ── Membres ──────────────────────────────────────────────
        elif val == "members":
            humans = [m for m in g.members if not m.bot]
            si = {discord.Status.online: "🟢", discord.Status.idle: "🟡",
                  discord.Status.dnd: "🔴", discord.Status.offline: "⚫"}
            e = discord.Embed(title="👥  Membres — " + g.name,
                              description="`" + str(len(humans)) + "` membre(s)",
                              color=C_GREEN)
            chunk = humans[:24]
            lines = [si.get(m.status, "⚫") + " **" + m.display_name + "**" for m in chunk]
            col_size = max(1, (len(lines) + 2) // 3)
            for ci in range(3):
                col = lines[ci * col_size:(ci + 1) * col_size]
                if col: e.add_field(name="\u200b", value="\n".join(col), inline=True)
            if len(humans) > 24: e.set_footer(text="…et " + str(len(humans) - 24) + " autres membres")
            await inter.response.send_message(embed=e, view=MemberActionView(g), ephemeral=True)

        # ── Rôles ────────────────────────────────────────────────
        elif val == "roles":
            roles = [r for r in g.roles if r.name != "@everyone"][::-1]
            e = discord.Embed(title="📋  Rôles — " + g.name,
                              description="`" + str(len(roles)) + "` rôle(s)",
                              color=C_BLURPLE)
            lines = [("🔵" if r.color.value else "⚪") + " " + r.mention + "  `" + str(len(r.members)) + " membres`"
                     for r in roles[:20]]
            e.description += "\n\n" + "\n".join(lines)
            if len(roles) > 20: e.set_footer(text="…et " + str(len(roles) - 20) + " autres rôles")
            await inter.response.send_message(embed=e, ephemeral=True)

        # ── Salons ───────────────────────────────────────────────
        elif val == "channels":
            e = discord.Embed(title="📡  Salons — " + g.name, color=C_TEAL)
            cats = sorted(g.categories, key=lambda c: c.position)
            for cat3 in cats[:6]:
                txt = [c for c in cat3.channels if isinstance(c, discord.TextChannel)]
                if not txt: continue
                val_str = "\n".join("#" + c.name for c in txt[:8])
                if len(txt) > 8: val_str += "\n*…+" + str(len(txt) - 8) + "*"
                e.add_field(name="📁 " + cat3.name, value=val_str, inline=True)
            await inter.response.send_message(embed=e, ephemeral=True)

        # ── Infos serveur ────────────────────────────────────────
        elif val == "server":
            humans = [m for m in g.members if not m.bot]
            online = [m for m in humans if m.status != discord.Status.offline]
            bots   = [m for m in g.members if m.bot]
            e = discord.Embed(title="🌐  " + g.name, color=C_DARK,
                              timestamp=datetime.now(timezone.utc))
            if g.icon: e.set_thumbnail(url=g.icon.url)
            e.add_field(name="👥  Membres",  value="`" + str(len(humans)) + "` total\n`" + str(len(online)) + "` en ligne", inline=True)
            e.add_field(name="🤖  Bots",     value="`" + str(len(bots)) + "`", inline=True)
            e.add_field(name="📋  Rôles",    value="`" + str(len(g.roles) - 1) + "`", inline=True)
            txt_chs = [c for c in g.channels if isinstance(c, discord.TextChannel)]
            e.add_field(name="📡  Salons",   value="`" + str(len(txt_chs)) + "` texte\n`" + str(len(g.categories)) + "` catégorie(s)", inline=True)
            e.add_field(name="📅  Création", value="<t:" + str(int(g.created_at.timestamp())) + ":D>", inline=True)
            e.add_field(name="🆔  ID",       value="`" + str(g.id) + "`", inline=True)
            await inter.response.send_message(embed=e, ephemeral=True)


# ── Vue : liste tickets + sélection ──────────────────────────────
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
class ConfigFullView(discord.ui.View):
    def __init__(self, guild, cfg, role_page=0):
        super().__init__(timeout=300)
        self.guild     = guild
        self.cfg       = cfg
        self.role_page = role_page
        cats  = sorted(guild.categories, key=lambda c: c.position)
        chs   = sorted([c for c in guild.channels if isinstance(c, discord.TextChannel)], key=lambda c: c.name)
        all_roles = [r for r in guild.roles if r.name != "@everyone"]

        if cats:
            s1 = discord.ui.Select(
                placeholder="📁  Catégorie des tickets…",
                options=[discord.SelectOption(
                    label=cat.name[:50],
                    description=str(len(cat.channels)) + " salon(s)",
                    value=str(cat.id),
                    default=(cfg.get("category_id") == cat.id)) for cat in cats[:25]],
                row=0)
            s1.callback = self.set_cat
            self.add_item(s1)

        if chs:
            s2 = discord.ui.Select(
                placeholder="📣  Salon des notifications staff…",
                options=[discord.SelectOption(
                    label=("#" + c.name)[:50],
                    value=str(c.id),
                    default=(cfg.get("staff_channel_id") == c.id)) for c in chs[:25]],
                row=1)
            s2.callback = self.set_sch
            self.add_item(s2)

        # Rôles paginés : page de 24 + option "page suivante" si besoin
        if all_roles:
            page_start = role_page * 24
            page_roles = all_roles[page_start:page_start + 24]
            has_next   = (page_start + 24) < len(all_roles)
            opts_r = [discord.SelectOption(
                label=r.name[:50],
                description=str(len(r.members)) + " membre(s)",
                value=str(r.id),
                default=(cfg.get("staff_role_id") == r.id)) for r in page_roles]
            if has_next:
                opts_r.append(discord.SelectOption(
                    label="➡️  Page suivante (" + str(len(all_roles) - page_start - 24) + " rôles restants)",
                    value="__next_page__",
                    description="Voir les rôles suivants"))
            suffix = "  (p." + str(role_page + 1) + ")" if len(all_roles) > 24 else ""
            s3 = discord.ui.Select(
                placeholder="🛡  Rôle staff autorisé" + suffix + "…",
                options=opts_r, row=2)
            s3.callback = self.set_sro
            self.add_item(s3)

        # Bouton saisie manuelle du rôle (pour les très grands serveurs)
        btn_role = discord.ui.Button(label="Saisir rôle (nom)", style=discord.ButtonStyle.secondary,
                                     emoji="🔍", row=3)
        btn_role.callback = self.manual_role
        self.add_item(btn_role)

        btn_show = discord.ui.Button(label="Config actuelle", style=discord.ButtonStyle.secondary,
                                     emoji="📊", row=3)
        btn_show.callback = self.show_cfg
        self.add_item(btn_show)

    async def set_cat(self, inter: discord.Interaction):
        if not is_staff(inter.user, self.guild.id):
            await inter.response.send_message(embed=_e_err("🚫"), ephemeral=True); return
        self.cfg["category_id"] = int(inter.data["values"][0])
        await save_data()
        cat = self.guild.get_channel(self.cfg["category_id"])
        await inter.response.send_message(
            embed=_e_ok("✅  Catégorie mise à jour", "**" + (cat.name if cat else "?") + "**"), ephemeral=True)

    async def set_sch(self, inter: discord.Interaction):
        if not is_staff(inter.user, self.guild.id):
            await inter.response.send_message(embed=_e_err("🚫"), ephemeral=True); return
        self.cfg["staff_channel_id"] = int(inter.data["values"][0])
        await save_data()
        ch = self.guild.get_channel(self.cfg["staff_channel_id"])
        await inter.response.send_message(
            embed=_e_ok("✅  Salon staff mis à jour", "**#" + (ch.name if ch else "?") + "**"), ephemeral=True)

    async def set_sro(self, inter: discord.Interaction):
        if not is_staff(inter.user, self.guild.id):
            await inter.response.send_message(embed=_e_err("🚫"), ephemeral=True); return
        val = inter.data["values"][0]
        if val == "__next_page__":
            new_view = ConfigFullView(self.guild, self.cfg, self.role_page + 1)
            await inter.response.edit_message(view=new_view)
            return
        self.cfg["staff_role_id"] = int(val)
        await save_data()
        r = self.guild.get_role(self.cfg["staff_role_id"])
        await inter.response.send_message(
            embed=_e_ok("✅  Rôle staff mis à jour", "**" + (r.name if r else "?") + "**"), ephemeral=True)

    async def manual_role(self, inter: discord.Interaction):
        if not is_staff(inter.user, self.guild.id):
            await inter.response.send_message(embed=_e_err("🚫"), ephemeral=True); return
        await inter.response.send_modal(RoleSearchModal(self.guild, self.cfg))

    async def show_cfg(self, inter: discord.Interaction):
        cat2 = self.guild.get_channel(self.cfg.get("category_id") or 0)
        sch2 = self.guild.get_channel(self.cfg.get("staff_channel_id") or 0)
        sro2 = self.guild.get_role(self.cfg.get("staff_role_id") or 0)
        ok_cfg, msg_cfg = is_configured(self.guild.id)
        e = discord.Embed(title="📊  Config — " + self.guild.name, color=C_BLUE)
        e.add_field(name="📁 Catégorie",   value="`" + cat2.name + "`" if cat2 else "❌", inline=True)
        e.add_field(name="📣 Salon staff", value=sch2.mention if sch2 else "❌",          inline=True)
        e.add_field(name="🛡 Rôle staff",  value=sro2.mention if sro2 else "⚠️",         inline=True)
        e.add_field(name="🔒 Menu Discord", value=("Verrouillé 🔒" if self.cfg.get("menu_locked") else "Libre 🔓") + "\n*Modifiable via console*", inline=True)
        if not ok_cfg: e.add_field(name="⚠️ Manquant", value=msg_cfg, inline=False)
        await inter.response.send_message(embed=e, ephemeral=True)


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
    global maintenance_mode, menu_locked
    try:
        return await _dispatch_inner(action, p)
    except Exception as e:
        import traceback
        log("API", f"Exception dans dispatch action='{action}' : {e}")
        log("API", traceback.format_exc())
        return {"error": f"Dispatch error: {e}"}

async def _dispatch_inner(action, p):
    global maintenance_mode, menu_locked

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
        if not YT_DLP_OK:
            return {"error": "yt_dlp non installé"}
        query     = p.get("query", "")
        ch_id     = p.get("np_channel_id")
        requester = p.get("requester", "Console")
        if not query: return {"error": "query requis"}
        tracks = await _resolve_query(query)
        if not tracks: return {"error": "Aucun résultat"}
        for t in tracks: t["requester"] = requester
        st = _mstate(g.id)
        st["np_channel"] = ch_id
        was_empty = not st["queue"] and st["current"] is None
        st["queue"].extend(tracks)
        if was_empty:
            vc = g.voice_client
            if vc and vc.is_connected() and not vc.is_playing():
                await _play_next(g)
        return {"ok": True, "added": len(tracks), "titles": [t["title"] for t in tracks[:5]]}

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

    return {"error": "Unknown action: " + action}


# ── Lancement ──────────────────────────────────────────────────────
async def main():
    log("BOOT", "=" * 52)
    log("BOOT", "Demarrage bot_agent.py")
    log("BOOT", "=" * 52)
    log("BOOT", f"PORT           = {PORT}")
    log("BOOT", f"REMOTE_ENABLED = {REMOTE_ENABLED}")
    log("BOOT", "DISCORD_TOKEN  = " + ("OK" if TOKEN else "MANQUANT"))
    log("BOOT", "DATABASE_URL   = " + ("OK (" + DATABASE_URL.split('@')[-1] + ")" if DATABASE_URL else "NON DEFINIE — persistance desactivee"))
    log("BOOT", "BOT_REMOTE_SECRET = " + ("OK" if REMOTE_SECRET != 'changeme' else "valeur par defaut (changeme)"))
    if not TOKEN:
        log("BOOT", "DISCORD_TOKEN manquant — arret.")
        sys.exit(1)
    log("HTTP", "Demarrage serveur HTTP...")
    app = web.Application()
    app.router.add_get("/health", handle_health)
    app.router.add_post("/remote", handle_remote)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    log("HTTP", f"Serveur HTTP actif sur 0.0.0.0:{PORT}")
    log("BOT", "Connexion a Discord...")
    await client.start(TOKEN)

if __name__ == "__main__":
    asyncio.run(main())
