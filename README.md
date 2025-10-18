# gate-alert

deployed as systemd service to watch liveness of gate
To update update gate_alert.py with desigerd changes 
push to git and then log in to VM and:
```bash
git pull
sudo systemctl stop gate-watchdog.service
sudo cp gate_alert.py /etc/gate/
sudo systemctl start  gate-watchdog.service
```

