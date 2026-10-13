package com.lanmesh.lanmesh_mobile

import android.Manifest
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Build
import androidx.annotation.NonNull
import androidx.core.app.ActivityCompat
import androidx.core.content.ContextCompat
import io.flutter.embedding.android.FlutterActivity
import io.flutter.embedding.engine.FlutterEngine
import io.flutter.plugin.common.MethodChannel

class MainActivity : FlutterActivity() {
    private val CHANNEL = "com.lanmesh/engine"
    private val PERMISSION_REQUEST_CODE = 1001

    override fun onResume() {
        super.onResume()
        requestRuntimePermissions()
    }

    private fun requestRuntimePermissions() {
        val permissions = mutableListOf(
            Manifest.permission.RECORD_AUDIO,
            Manifest.permission.CAMERA,
            Manifest.permission.ACCESS_FINE_LOCATION,
            Manifest.permission.ACCESS_COARSE_LOCATION
        )
        // Android 13+ (API 33+): granular media permissions for file/image picker
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            permissions.add(Manifest.permission.POST_NOTIFICATIONS)
            permissions.add(Manifest.permission.READ_MEDIA_IMAGES)
            permissions.add(Manifest.permission.READ_MEDIA_VIDEO)
            permissions.add(Manifest.permission.READ_MEDIA_AUDIO)
        } else {
            // Legacy storage permission for Android 12 and below
            permissions.add(Manifest.permission.READ_EXTERNAL_STORAGE)
        }

        val needed = permissions.filter {
            ContextCompat.checkSelfPermission(this, it) != PackageManager.PERMISSION_GRANTED
        }

        if (needed.isNotEmpty()) {
            ActivityCompat.requestPermissions(this, needed.toTypedArray(), PERMISSION_REQUEST_CODE)
        }
    }

    override fun configureFlutterEngine(@NonNull flutterEngine: FlutterEngine) {
        super.configureFlutterEngine(flutterEngine)

        MethodChannel(flutterEngine.dartExecutor.binaryMessenger, CHANNEL).setMethodCallHandler { call, result ->
            when (call.method) {
                "startNode" -> {
                    requestRuntimePermissions()
                    val intent = Intent(this, MeshService::class.java)
                    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                        startForegroundService(intent)
                    } else {
                        startService(intent)
                    }
                    result.success(true)
                }
                "stopNode" -> {
                    val intent = Intent(this, MeshService::class.java).apply {
                        action = MeshService.ACTION_STOP
                    }
                    startService(intent)
                    result.success(true)
                }
                "getNodeStatus" -> {
                    val statusMap = mapOf(
                        "isRunning" to MeshService.isNodeRunning,
                        "peerId" to MeshService.peerId,
                        "port" to MeshService.NODE_PORT,
                        "lastStatus" to MeshService.lastStatus,
                        "logs" to MeshService.recentLogs.joinToString("\n")
                    )
                    result.success(statusMap)
                }
                else -> {
                    result.notImplemented()
                }
            }
        }
    }
}
