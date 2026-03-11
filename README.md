# Discord Bot — Remote Control Architecture

Bot Discord hébergé sur Render, contrôlable depuis une console distante.

## Architecture

```
remote_console.py  ──(HTTPS POST /remote)──►  bot_agent.py (Render)
   ta machine                                    ├── Discord
                                                 ├── /health  (UptimeRobot)
                                                 └── bot_data.json
```

## Démarrage rapide

### 1. Bot Agent (Render)

1. Push le repo sur GitHub
2. Crée un **Web Service** sur [render.com](https://render.com)
3. Connecte ton repo GitHub
4. Render détecte `render.yaml` automatiquement
5. Ajoute les variables d'env dans le dashboard Render :
   - `DISCORD_TOKEN` → ton token Discord
   - `BOT_REMOTE_SECRET` → un secret fort (ex: `openssl rand -hex 32`)
   - `BOT_REMOTE_ENABLED` → `true`
6. Deploy → note l'URL `https://ton-bot.onrender.com`

### 2. Console distante (ta machine)

```bash
pip install requests python-dotenv
```

Crée un fichier `.env` :
```env
BOT_REMOTE_URL=https://ton-bot.onrender.com
BOT_REMOTE_SECRET=le_meme_secret_que_sur_render
```

Lance :
```bash
python remote_console.py
```

### 3. UptimeRobot

Ajoute un monitor HTTP sur :
```
https://ton-bot.onrender.com/health
```
Intervalle : 5 minutes (évite le sleep de Render Free).

---

## Commandes console distante

| Commande | Description |
|---|---|
| `status` | État du bot |
| `guilds` | Liste des serveurs |
| `use <n°>` | Sélectionner un serveur |
| `maintenance on\|off` | Mode maintenance *(console only)* |
| `lockmenu / unlockmenu` | Verrouiller le menu Discord *(console only)* |
| `basecfg` | Configurer catégorie/staff (interactif) |
| `ttype add\|edit\|delete` | Gérer les types de tickets |
| `tpending` | Salons en attente |
| `tlaunch <n°> <type>` | Lancer les questions |
| `topen <membre> <type>` | Créer un ticket |
| `taccept\|trefuse\|tclose <n°>` | Actions tickets |
| `tsend <n°> <msg>` | Envoyer dans un ticket |
| `members\|roles\|channels` | Infos serveur |
| `kick\|ban <membre>` | Modération |

## Commandes Discord (staff)

```
!menu          → menu interactif (embeds + boutons + selects)
!ticket [type] → ouvrir un ticket (tout le monde)
!tpending      → salons en attente
!tlaunch <n°> <type>
!tlist [open|closed|all]
!taccept <n°>  !trefuse <n°>  !tclose <n°>
!tsend <n°> <message>
!thelp
```

## Permissions Discord bot

Le bot a besoin de :
- `Gérer les salons`
- `Gérer les messages`
- `Voir les salons`
- `Envoyer des messages`
- `Lire l'historique`
- `Ajouter des réactions`
- `Expulser / Bannir des membres`
- `Intents : Members + Message Content + Presence`

## Sécurité

Chaque requête `/remote` est signée avec HMAC-SHA256 :
```
X-Signature: sha256(BOT_REMOTE_SECRET, body)
```
Le secret doit être **identique** sur le bot et la console.
