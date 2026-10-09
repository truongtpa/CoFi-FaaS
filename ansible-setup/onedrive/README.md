# Backup lên OneDrive cá nhân

## 1. Lấy token ở máy local

Cài rclone (macOS: `brew install rclone`) rồi chạy:

```bash
rclone authorize "onedrive"
```

## 2. Tạo `secrets.yml`

```
Open file secrets.yml and update the onedrive token
onedrive_token: '{"access_token":"EwB...","token_type":"Bearer","refresh_token":"M.C...","expiry":"..."}'
```

## 3. Tải từ server leen OneDrive

```bash
ansible-playbook backup-onedrive.yml -e @secrets.yml
```

## 4. Tải từ OneDrive về server

```bash
ansible-playbook download-onedrive.yml -e @secrets.yml
```

## 5. Linux CLI

```bash
pgrep -a "^(tar|rclone)"
awk "/^rchar/{print int(\$2*100/2608600070)\"%\"}" /proc/$(pgrep -f "^rclone copyto")/io
zip -r BLOOM-FaaS CoFi-FaaS-8-Oct.zip
```