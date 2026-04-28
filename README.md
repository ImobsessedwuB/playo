# Cantex Swap Bot

Bot swap otomatis untuk pair USDCx <-> cBTC di Cantex Exchange.

## Arsitektur

```
Railway #1 (FEE_WATCHER=true)
  - Fee Watcher: monitor gas fee via WS + REST
  - Signal Server: HTTP endpoint untuk kirim trigger ke executor
  - Telegram Panel: admin control

Railway #2, #3, #4 (FEE_WATCHER=false)
  - Executor: menunggu sinyal, eksekusi swap paralel semua akun
  - Telegram Panel: terima summary
```

## Logic Swap Harian

- Pair: USDCx <-> cBTC (bolak-balik)
- 6 TX per hari:
  - TX 1-4: all-in seluruh saldo (menyisakan CC untuk gas fee)
  - TX 5-6: fixed $2 per transaksi
- Setelah 6 TX selesai, bot tidur sampai keesokan harinya (pukul 00:05 UTC)

## Setup Railway

### 1. Environment Variables

**Fee Watcher Railway (satu instance):**
```
FEE_WATCHER=true
TELEGRAM_BOT_TOKEN=<token bot telegram>
PORT=8080
LOG_LEVEL=INFO
DRY_RUN=false
RAILWAY_VOLUME_MOUNT_PATH=/data
CANTEX_BASE_URL=https://api.cantex.io
```

**Executor Railway (3 instance lainnya):**
```
FEE_WATCHER=false
TELEGRAM_BOT_TOKEN=<token bot telegram>
FEE_WATCHER_URL=https://<url-railway-fee-watcher>/
LOG_LEVEL=INFO
DRY_RUN=false
RAILWAY_VOLUME_MOUNT_PATH=/data
CANTEX_BASE_URL=https://api.cantex.io
```

### 2. Railway Volume

Setiap Railway instance butuh Volume yang di-mount ke `/data`.
Semua credentials dan progress disimpan di volume ini.

**Cara set di Railway:**
- Buka project -> Service -> Storage -> Add Volume
- Mount path: `/data`

### 3. Deploy

1. Push repo ke GitHub (tanpa credentials, tanpa .env)
2. Buat 4 service di Railway dari repo yang sama
3. Set environment variables sesuai tabel di atas
4. Tambahkan volume ke masing-masing service
5. Deploy

## Penggunaan Telegram

### Admin (user ID 6469077855)

Ketik `/start` atau `/menu` untuk membuka panel kontrol.

**Menu yang tersedia:**

| Menu | Fungsi |
|---|---|
| Input Credentials Canton | Tambah akun, input operator key + trading key |
| Set Proxy / Change Proxy | Set proxy per akun (format: `user:pass@ip:port`) |
| Set Fee Watcher Cookie | Input cookie Cantex untuk fee watcher |
| Set Max Fee (CC) | Set batas maksimum fee, contoh: `0.27` |
| Lihat Status | Status akun dan progress hari ini |
| Lihat Summary | Summary transaksi hari ini |
| Hapus Semua Akun | Hapus semua credentials |

**Flow input akun:**
1. Klik "Input Credentials Canton"
2. Bot tanya: "Berapa akun?" -> jawab angka, misal: `5`
3. Input credentials berurutan, format 2 baris:
   ```
   <operator_key>
   <trading_key>
   ```
4. Ulangi untuk setiap akun

**Set proxy:**
1. Klik "Set Proxy"
2. Input per akun, format: `username:password@ip:port`
3. Ketik `skip` jika akun ini tidak pakai proxy

**Set fee watcher:**
1. Klik "Set Fee Watcher Cookie"
2. Paste cookie dari browser Cantex
   - Buka cantex.io -> F12 -> Network -> copy header `cookie` dari request apapun
3. Bot otomatis hapus pesan cookie untuk keamanan

**Set max fee:**
1. Klik "Set Max Fee (CC)"
2. Input nilai, contoh: `0.27`
3. Bot hanya eksekusi jika fee <= nilai ini

### Non-Admin

Hanya bisa `/start` untuk subscribe summary.
Akan menerima notifikasi setelah semua transaksi selesai.

## Format Proxy

```
username:password@ip:port
```

Contoh:
```
user123:pass456@192.168.1.1:8080
```

## Struktur File di Volume Railway

```
/data/
  credentials.json    # operator key + trading key per akun (terenkripsi di volume)
  fee_watcher.json    # cookie fee watcher + max fee setting
  progress.json       # progress tx harian per akun
  fee_signal.json     # sinyal fee trigger (dipakai jika shared volume)
```

## Catatan Keamanan

- Credentials disimpan di Railway Volume, bukan di GitHub
- Cookie fee watcher otomatis dihapus dari chat Telegram setelah diinput
- Jangan pernah commit file `.env` atau `credentials.json` ke GitHub

## Multi-Railway: Pembagian Akun

Untuk membagi beban akun di 4 Railway:
- Input akun yang berbeda di masing-masing Railway via Telegram bot
- Fee watcher mengirim sinyal ke semua executor via `FEE_WATCHER_URL`
- Setiap executor menjalankan akun miliknya sendiri secara paralel

## Commands Telegram

| Command | Fungsi |
|---|---|
| `/start` | Mulai & subscribe notifikasi |
| `/menu` | Buka menu utama (admin only) |
| `/status` | Lihat status akun (admin only) |
| `/summary` | Lihat summary hari ini |
| `/cancel` | Batalkan input yang sedang berjalan |
