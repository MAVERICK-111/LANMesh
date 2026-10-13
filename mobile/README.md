# LANMesh Mobile (Android Peer Client)

LANMesh Mobile turns any Android smartphone into an **autonomous peer-to-peer node** in the LANMesh network. It runs an embedded, native pure Go engine (`ARM64 ELF`) in an Android Foreground Service with full local storage, multi-hop relaying, and dual mDNS/UDP auto-discovery.

---

## Architecture Overview

```
┌────────────────────────────────────────────────────────┐
│                   LANMesh Mobile                       │
│                                                        │
│  ┌────────────────────────┐  ┌──────────────────────┐  │
│  │   Flutter Dart UI      │  │  Android Foreground  │  │
│  │  - WebView Controller  │  │       Service        │  │
│  │  - Status Monitor      │  │  - MulticastLock     │  │
│  │  - Material 3 Dark UI  │  │  - WakeLock          │  │
│  └───────────┬────────────┘  │  - Process Lifecycle │  │
│              │ HTTP / WS     └──────────┬───────────┘  │
│              │ (127.0.0.1:port)         │ Manages      │
│              ▼                          ▼              │
│  ┌──────────────────────────────────────────────────┐  │
│  │     Embedded Native Go Engine (liblanmesh.so)    │  │
│  │     - Libp2p Host & Multi-hop Relay              │  │
│  │     - BoltDB Local Persistence (lanmesh.db)      │  │
│  │     - UDP Broadcast Beacon (port 9999)           │  │
│  │     - Two-Phase Digest Sync (/p2p-sync/1.0.0)    │  │
│  └──────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────┘
```

---

## Prerequisites

1. **Flutter SDK:** 3.13+ or later (Dart 3.1+)
2. **Android SDK:** Platform API 34+ / Android Build Tools 34+
3. **Go Toolchain:** Go 1.23+ (for cross-compiling the embedded engine)
4. **Android Device:** ARM64 (aarch64) device running Android 8.0+ (API 26+)

---

## Dependencies & Setup

Install the required Flutter packages:

```bash
flutter pub get
```

Key dependencies in `pubspec.yaml`:
* **`webview_flutter: ^4.14.1`** — High-performance native Android WebView displaying the rich LANMesh chat UI.
* **`http: ^1.6.0`** — Client health checks and backend daemon readiness probing.
* **`cupertino_icons: ^1.0.8`** — iOS/Material iconography.

---

## Build & Release Workflow

### 1. Compile Embedded Go Engine (ARM64)
From the root repository directory:
```powershell
# Windows (PowerShell)
$env:GOOS = "android"; $env:GOARCH = "arm64"; $env:CGO_ENABLED = "0"
go build -ldflags="-checklinkname=0" -o lanmesh-android .

# Linux / macOS (Bash)
GOOS=android GOARCH=arm64 CGO_ENABLED=0 go build -ldflags="-checklinkname=0" -o lanmesh-android .
```

Copy the compiled engine into the Android native library folder and assets:
```powershell
Copy-Item "..\lanmesh-android" "android\app\src\main\jniLibs\arm64-v8a\liblanmesh.so" -Force
Copy-Item "..\lanmesh-android" "android\app\src\main\assets\lanmesh-android" -Force
```

### 2. Build Signed Release APK
From the `mobile/` directory:
```bash
flutter build apk --release
```

The signed release APK will be located at:
`build/app/outputs/flutter-apk/app-release.apk`

---

## Running on Device

### Option A: Install via ADB
```bash
adb install -r build/app/outputs/flutter-apk/app-release.apk
```

### Option B: Run in Debug Mode via Flutter
```bash
flutter run
```

---

## Release Signing Configuration

Release signing is pre-configured via:
* **Keystore:** `android/lanmesh-release.jks` (2048-bit RSA)
* **Properties:** `android/key.properties`
* **Gradle Integration:** `android/app/build.gradle.kts`
