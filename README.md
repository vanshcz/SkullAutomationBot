# 🤖 SkullAutomationBot

A modular Python Telegram bot built for automation, account handling, and admin-controlled workflows.

> ⚠️ **Important Notice**
> This project is a **testing / educational version only**.
> It is designed to demonstrate how Telegram bots and Python automation systems work.
> **Not intended for production or misuse.**

---

## 🚀 Overview

**SkullAutomationBot** is a fully working Telegram bot converted from a single-file script into a clean multi-file project structure.

It demonstrates:

* Modular Python architecture
* Telegram bot handling
* Session-based workflows
* Admin control panels
* Background workers
* Force-join systems

---

## 🧠 Features

### 👤 User Side

* Flexible phone number input (accepts any format)
* OTP input normalization (auto-cleans spaces/dashes)
* Telegram login workflow
* Smooth step-by-step interaction

### 🔒 Force Join System

* Users must join required channels before using the bot
* "Join Now" buttons + "Check Again"
* Fully controlled by admin panel

### 🛠 Admin Panel

* Add / Remove force-join channels
* Toggle force join ON/OFF
* Maintenance mode ON/OFF
* System status overview

### ⚙️ Backend

* Multi-file modular structure
* Database integration
* Background workers
* Logging system

---

## 📁 Project Structure

```
SkullAutomationBot/
│
├── main.py                # Entry point (runs bot.infinity_polling)
├── config.py              # Configuration variables
├── database.py            # Database logic
├── bot_handlers.py        # Telegram handlers
├── helpers.py             # Utility functions
├── client_runtime.py      # Telegram client logic
├── workers.py             # Background tasks
├── shared.py              # Shared state
├── app_logger.py          # Logging system
│
├── requirements.txt
├── Procfile
├── runtime.txt
└── nixpacks.toml
```

---

## ⚡ Installation

### 1. Clone Repo

```bash
git clone https://github.com/vanshcz/SkullAutomationBot.git
cd SkullAutomationBot
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Setup Config

Edit `config.py` and add:

```python
BOT_TOKEN = "YOUR_BOT_TOKEN"
API_ID = 123456
API_HASH = "YOUR_API_HASH"
ADMIN_IDS = [123456789]
```

---

## ▶️ Run the Bot

```bash
python main.py
```

Bot will start using:

```python
bot.infinity_polling()
```

---

## 🔐 Force Join Setup

From **Admin Panel**:

* Add channel:

  ```
  @channelusername
  ```

* OR private channel:

  ```
  -1001234567890 | https://t.me/+invite | Channel Name
  ```

> ⚠️ Bot must be admin in private channels

---

## 🧪 Testing Purpose

This project is:

* ✔ For **learning Python bot development**
* ✔ For **understanding Telegram automation**
* ✔ For **testing modular architecture**

This is **NOT**:

* ❌ A commercial bot
* ❌ A secure production system
* ❌ Meant for abuse or spam

---

## 👨‍💻 Developer

**Vansh** 👑

* 🌐 Website: https://vanshcz.online
* 📺 YouTube: https://youtube.com/@vanshcz
* 📸 Instagram: https://instagram.com/vanshcz
* 💬 Telegram: https://t.me/skullmoddder
* 🤖 Bots Channel: https://t.me/botsarefather
* 🐙 GitHub: https://github.com/vanshcz

---

## 📌 Repository

**Repo Name:** `SkullAutomationBot`

---

## ⚠️ Disclaimer

This project is created **only for educational purposes**.

Any misuse of this code is **not the responsibility of the developer**.

Use responsibly and respect Telegram's terms of service.

---

## ❤️ Credits

Developed and launched by **Vansh**
Made for educational exploration of Python automation 🚀

---
