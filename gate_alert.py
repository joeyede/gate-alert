import os
import json
import time
import threading
import ssl
import logging
from typing import Optional
from datetime import datetime, timedelta

import paho.mqtt.client as mqtt
import requests
from dotenv import load_dotenv

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
)

class MQTTWatchdog:
    """
    Monitors an MQTT topic for heartbeat messages and sends a Telegram alert
    if the heartbeat stops. It also sends a recovery notification.
    """

    def __init__(self):
        """Initializes the watchdog, loading configuration and setting up the MQTT client."""
        load_dotenv()  # Load environment variables from .env file

        # --- Configuration ---
        self.broker_url = "3b62666a86a14b23956244c4308bad76.s1.eu.hivemq.cloud"
        self.port = 8883
        self.topic = "gate/status"
        self.username = "pgate"
        self.timeout = 600  # seconds

        # --- Secrets & Environment-specific Config ---
        self.password = os.getenv("MQTT_PASS")
        self.tg_bot_token = os.getenv("TG_BOT_TOKEN")
        self.tg_chat_id = os.getenv("TG_CHAT_ID")

        if not all([self.password, self.tg_bot_token, self.tg_chat_id]):
            raise ValueError(
                "Missing required environment variables. Ensure MQTT_PASS, "
                "TG_BOT_TOKEN, and TG_CHAT_ID are set in your environment or a .env file."
            )

        # --- State ---
        self.timer: Optional[threading.Timer] = None
        self.daily_timer: Optional[threading.Timer] = None
        self.alert_active = False

        # --- MQTT Client Setup ---
        try:
            # For paho-mqtt v2.0.0+
            self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
        except AttributeError:
            # For paho-mqtt v1.x
            logging.warning("paho-mqtt < 2.0.0 detected, using legacy client initialization.")
            self.client = mqtt.Client(protocol=mqtt.MQTTv5)

        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.username_pw_set(self.username, self.password)
        self.client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
        self.client.tls_insecure_set(False)

    def _send_telegram(self, text: str):
        """Sends a message to the configured Telegram chat."""
        logging.info(f"Sending Telegram message: {text.splitlines()[0]}")
        try:
            url = f"https://api.telegram.org/bot{self.tg_bot_token}/sendMessage"
            response = requests.post(
                url,
                json={
                    "chat_id": self.tg_chat_id,
                    "text": text,
                    "parse_mode": "Markdown",
                    "disable_web_page_preview": True,
                },
                timeout=10,
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logging.error(f"Telegram send failed: {e}", exc_info=True)

    def _send_daily_checkin(self):
        """Sends a daily 'still alive' message and reschedules itself."""
        timestamp = datetime.now().isoformat(sep=' ', timespec='seconds')
        msg = f"👍 *Daily Check-in*\nThe MQTT Watchdog script is running correctly.\n_Check-in at `{timestamp}`_"
        logging.info("Sending daily check-in message.")
        self._send_telegram(msg)
        self._schedule_daily_checkin()  # Reschedule for the next day

    def _schedule_daily_checkin(self):
        """Calculates time until next 12:15 PM and schedules the check-in message."""
        if self.daily_timer:
            self.daily_timer.cancel()

        now = datetime.now()
        # Set the target time for today
        checkin_time = now.replace(hour=12, minute=15, second=0, microsecond=0)

        if now >= checkin_time:
            # If it's already past 12:15 PM today, schedule for tomorrow
            checkin_time += timedelta(days=1)

        delay_seconds = (checkin_time - now).total_seconds()

        self.daily_timer = threading.Timer(delay_seconds, self._send_daily_checkin)
        self.daily_timer.daemon = True
        self.daily_timer.start()
        logging.info(
            f"Scheduled next daily check-in for {checkin_time.strftime('%Y-%m-%d %H:%M:%S')}"
        )

    def _trigger_alert(self):
        """Triggers an alert if one is not already active."""
        if not self.alert_active:
            self.alert_active = True
            msg = f"🚨 *Gate heartbeat missing*\nNo message on `{self.topic}` for > {self.timeout}s."
            timestamp = datetime.now().isoformat(sep=' ', timespec='seconds')
            msg = f"🚨 *Gate heartbeat missing*\nNo message on `{self.topic}` for > {self.timeout}s.\n_Alert triggered at `{timestamp}`_"
            logging.warning(msg)
            self._send_telegram(msg)

    def _trigger_recovery(self, ts: Optional[str]):
        """Sends a recovery message if an alert was active."""
        if self.alert_active:
            timestamp = datetime.now().isoformat(sep=' ', timespec='seconds')
            self.alert_active = False
            ts_str = f"`{ts}`" if ts else "N/A"
            msg = f"✅ *Gate heartbeat restored*\nLast received: {ts_str}."
            msg = f"✅ *Gate heartbeat restored*\nLast received: {ts_str}.\n_Recovered at `{timestamp}`_"
            logging.info(msg)
            self._send_telegram(msg)

    def _reset_timer(self):
        """Cancels the existing timer and starts a new one."""
        if self.timer:
            self.timer.cancel()
        self.timer = threading.Timer(self.timeout, self._trigger_alert)
        self.timer.daemon = True
        self.timer.start()

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        """Callback for when the client connects to the broker."""
        if reason_code == 0:
            logging.info(f"Successfully connected to broker. Subscribing to '{self.topic}'.")
            client.subscribe(self.topic)
            self._reset_timer()
        else:
            logging.error(f"Connection to broker failed with code: {reason_code}")

    def _on_message(self, client, userdata, msg):
        """Callback for when a message is received."""
        ts = None
        try:
            payload = json.loads(msg.payload.decode())
            ts = payload.get("hb")
            if ts:
                logging.info(f"Heartbeat received: {ts}")
            else:
                logging.warning("Heartbeat message received without 'hb' field.")
        except (json.JSONDecodeError, AttributeError) as e:
            logging.warning(f"Malformed message received on topic '{msg.topic}': {e}")

        # A message (even malformed) means the connection is alive.
        self._reset_timer()
        self._trigger_recovery(ts)

    def run(self):
        """Connects the MQTT client and starts the blocking loop."""
        logging.info("Starting MQTT Watchdog...")
        timestamp = datetime.now().isoformat(sep=' ', timespec='seconds')
        self._send_telegram(f"🚀 *MQTT Watchdog Started*\nMonitoring for gate heartbeats.\n_Started at `{timestamp}`_")
        self._schedule_daily_checkin()
        try:
            self.client.connect(self.broker_url, self.port, keepalive=60)
            self.client.loop_forever()  # This is a blocking call.
        except KeyboardInterrupt:
            logging.info("Shutdown signal received.")
        except Exception as e:
            logging.critical(f"A critical error occurred: {e}", exc_info=True)
        finally:
            logging.info("Cleaning up and shutting down.")
            if self.timer:
                self.timer.cancel()
            if self.daily_timer:
                self.daily_timer.cancel()
            if self.client.is_connected():
                self.client.disconnect()

if __name__ == "__main__":
    try:
        watchdog = MQTTWatchdog()
        watchdog.run()
    except ValueError as e:
        logging.error(e)
