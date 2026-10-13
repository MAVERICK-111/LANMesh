package com.lanmesh.lanmesh_mobile

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.net.wifi.WifiManager
import android.os.Build
import android.os.IBinder
import android.os.PowerManager
import android.util.Log
import androidx.core.app.NotificationCompat
import java.io.BufferedReader
import java.io.File
import java.io.FileOutputStream
import java.io.InputStreamReader

class MeshService : Service() {

    private var multicastLock: WifiManager.MulticastLock? = null
    private var wakeLock: PowerManager.WakeLock? = null
    private var daemonProcess: Process? = null
    private var logThread: Thread? = null

    companion object {
        private const val TAG = "LANMeshDaemon"
        const val NOTIFICATION_ID = 2001
        const val CHANNEL_ID = "lanmesh_node_channel"
        const val ACTION_STOP = "com.lanmesh.ACTION_STOP"
        const val NODE_PORT = 8080

        @Volatile
        var isNodeRunning: Boolean = false

        @Volatile
        var peerId: String = ""

        @Volatile
        var lastStatus: String = "Initializing..."

        val recentLogs = java.util.concurrent.CopyOnWriteArrayList<String>()
    }

    override fun onCreate() {
        super.onCreate()
        createNotificationChannel()
        acquireLocks()
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        if (intent?.action == ACTION_STOP) {
            Log.i(TAG, "Stop command received, halting MeshService")
            stopSelf()
            return START_NOT_STICKY
        }

        startForeground(NOTIFICATION_ID, buildNotification("LANMesh Node Starting..."))

        if (daemonProcess == null) {
            startDaemon()
        }

        return START_STICKY
    }

    private fun acquireLocks() {
        // 1. Unblock mDNS UDP broadcast/multicast (224.0.0.251:5353)
        try {
            val wifi = applicationContext.getSystemService(Context.WIFI_SERVICE) as WifiManager
            multicastLock = wifi.createMulticastLock("LANMeshMdnsLock").apply {
                setReferenceCounted(true)
                acquire()
            }
            Log.i(TAG, "MulticastLock acquired")
        } catch (e: Exception) {
            Log.e(TAG, "Failed to acquire MulticastLock: ${e.message}", e)
        }

        // 2. Prevent CPU sleep when phone is locked
        try {
            val pm = getSystemService(Context.POWER_SERVICE) as PowerManager
            wakeLock = pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "LANMesh:RelayWakeLock").apply {
                acquire()
            }
            Log.i(TAG, "WakeLock acquired")
        } catch (e: Exception) {
            Log.e(TAG, "Failed to acquire WakeLock: ${e.message}", e)
        }
    }

    private fun locateExecutable(): File {
        // Check extracted native library first
        val nativeLib = File(applicationInfo.nativeLibraryDir, "liblanmesh.so")
        if (nativeLib.exists() && nativeLib.canExecute()) {
            Log.i(TAG, "Using native library binary: ${nativeLib.absolutePath}")
            return nativeLib
        }

        // Fallback: extract from assets
        val internalBinary = File(filesDir, "lanmesh-android")
        Log.i(TAG, "Extracting asset to internal storage: ${internalBinary.absolutePath}")
        try {
            assets.open("lanmesh-android").use { input ->
                FileOutputStream(internalBinary).use { output ->
                    input.copyTo(output)
                }
            }
            internalBinary.setExecutable(true, false)
            Runtime.getRuntime().exec(arrayOf("chmod", "755", internalBinary.absolutePath)).waitFor()
        } catch (e: Exception) {
            Log.e(TAG, "Failed extracting asset binary: ${e.message}", e)
        }

        return internalBinary
    }

    private fun startDaemon() {
        Thread {
            try {
                val binaryFile = locateExecutable()
                val dataDir = File(filesDir, "meshdata").apply { mkdirs() }
                val nickname = Build.MODEL.replace("\\s+".toRegex(), "")

                Log.i(TAG, "Starting binary: ${binaryFile.absolutePath} -data-dir ${dataDir.absolutePath} -port $NODE_PORT -nickname $nickname")

                val pb = ProcessBuilder(
                    binaryFile.absolutePath,
                    "-data-dir", dataDir.absolutePath,
                    "-port", NODE_PORT.toString(),
                    "-nickname", nickname
                )
                pb.redirectErrorStream(true)
                val process = pb.start()
                daemonProcess = process

                logThread = Thread {
                    val reader = BufferedReader(InputStreamReader(process.inputStream))
                    var line: String?
                    while (reader.readLine().also { line = it } != null) {
                        val currentLine = line ?: continue
                        Log.d(TAG, currentLine)
                        lastStatus = currentLine
                        recentLogs.add(currentLine)
                        if (recentLogs.size > 100) {
                            recentLogs.removeAt(0)
                        }

                        if (currentLine.contains("Your Peer ID:")) {
                            peerId = currentLine.substringAfter("Your Peer ID:").trim()
                            isNodeRunning = true
                            updateNotification("Active • Peer ID: ${peerId.take(10)}...")
                        } else if (currentLine.contains("serving") || currentLine.contains("listening")) {
                            isNodeRunning = true
                            updateNotification("Active • Wi-Fi Mesh Relay Running")
                        }
                    }
                    Log.w(TAG, "Daemon process terminated")
                    isNodeRunning = false
                }
                logThread?.start()

            } catch (e: Exception) {
                Log.e(TAG, "Failed launching daemon: ${e.message}", e)
                isNodeRunning = false
            }
        }.start()
    }

    private fun createNotificationChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val channel = NotificationChannel(
                CHANNEL_ID,
                "LANMesh Peer Service",
                NotificationManager.IMPORTANCE_LOW
            ).apply {
                description = "Maintains local Wi-Fi mesh routing and peer discovery"
                setShowBadge(false)
            }
            val manager = getSystemService(NotificationManager::class.java)
            manager.createNotificationChannel(channel)
        }
    }

    private fun buildNotification(status: String): Notification {
        val launchIntent = Intent(this, MainActivity::class.java)
        val pendingLaunch = PendingIntent.getActivity(
            this,
            0,
            launchIntent,
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )

        val stopIntent = Intent(this, MeshService::class.java).apply {
            action = ACTION_STOP
        }
        val pendingStop = PendingIntent.getService(
            this,
            1,
            stopIntent,
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )

        return NotificationCompat.Builder(this, CHANNEL_ID)
            .setContentTitle("LANMesh P2P Engine")
            .setContentText(status)
            .setSmallIcon(android.R.drawable.stat_notify_sync)
            .setContentIntent(pendingLaunch)
            .addAction(android.R.drawable.ic_menu_close_clear_cancel, "Stop", pendingStop)
            .setOngoing(true)
            .setPriority(NotificationCompat.PRIORITY_LOW)
            .build()
    }

    private fun updateNotification(status: String) {
        val manager = getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        manager.notify(NOTIFICATION_ID, buildNotification(status))
    }

    override fun onDestroy() {
        super.onDestroy()
        isNodeRunning = false
        try {
            daemonProcess?.destroy()
            daemonProcess = null
        } catch (e: Exception) {
            Log.e(TAG, "Error stopping daemon: ${e.message}", e)
        }

        if (multicastLock?.isHeld == true) {
            try {
                multicastLock?.release()
            } catch (_: Exception) {}
        }
        if (wakeLock?.isHeld == true) {
            try {
                wakeLock?.release()
            } catch (_: Exception) {}
        }
    }

    override fun onBind(intent: Intent?): IBinder? = null
}
