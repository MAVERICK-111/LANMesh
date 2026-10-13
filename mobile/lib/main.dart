import 'dart:async';
import 'dart:io';

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:http/http.dart' as http;
import 'package:file_picker/file_picker.dart';
import 'package:webview_flutter/webview_flutter.dart';
import 'package:webview_flutter_android/webview_flutter_android.dart';

void main() {
  WidgetsFlutterBinding.ensureInitialized();
  SystemChrome.setSystemUIOverlayStyle(
    const SystemUiOverlayStyle(
      statusBarColor: Color(0xFF141414),
      statusBarIconBrightness: Brightness.light,
      systemNavigationBarColor: Color(0xFF141414),
      systemNavigationBarIconBrightness: Brightness.light,
    ),
  );
  runApp(const LanMeshApp());
}

class LanMeshApp extends StatelessWidget {
  const LanMeshApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'LANMesh',
      debugShowCheckedModeBanner: false,
      theme: ThemeData(
        brightness: Brightness.dark,
        scaffoldBackgroundColor: const Color(0xFF0B0B0C),
        primaryColor: const Color(0xFFF2C230),
        colorScheme: const ColorScheme.dark(
          primary: Color(0xFFF2C230),
          surface: Color(0xFF141414),
        ),
        fontFamily: 'Roboto',
      ),
      home: const MeshHomeScreen(),
    );
  }
}

class MeshHomeScreen extends StatefulWidget {
  const MeshHomeScreen({super.key});

  @override
  State<MeshHomeScreen> createState() => _MeshHomeScreenState();
}

class _MeshHomeScreenState extends State<MeshHomeScreen> {
  static const MethodChannel _platform = MethodChannel('com.lanmesh/engine');

  late final WebViewController _webViewController;
  bool _isNodeReady = false;
  bool _isEngineRunning = false;
  String _peerId = '';
  String _lastStatus = 'Booting libp2p engine...';
  String _allLogs = '';
  int _port = 8080;
  Timer? _statusTimer;
  Timer? _healthTimer;

  @override
  void initState() {
    super.initState();
    _initWebView();
    _startPeerDaemon();
    _startStatusMonitoring();
  }

  void _initWebView() {
    _webViewController = WebViewController()
      ..setJavaScriptMode(JavaScriptMode.unrestricted)
      ..setBackgroundColor(const Color(0xFF0B0B0C))
      ..setNavigationDelegate(
        NavigationDelegate(
          onPageFinished: (url) {
            if (url.contains('127.0.0.1') || url.contains('localhost')) {
              setState(() {
                _isNodeReady = true;
              });
            }
          },
          onWebResourceError: (error) {
            // If connection refused during initial node boot, keep waiting
            if (error.errorCode == -2 ||
                error.description.contains('net::ERR_CONNECTION_REFUSED')) {
              setState(() {
                _isNodeReady = false;
              });
            }
          },
        ),
      );

    // Android-specific WebView capabilities
    if (Platform.isAndroid &&
        _webViewController.platform is AndroidWebViewController) {
      final androidController =
          _webViewController.platform as AndroidWebViewController;

      // 1. Grant mic/camera/WebRTC permissions automatically (voice notes, video calls)
      androidController.setOnPlatformPermissionRequest((request) {
        request.grant();
      });

      // 2. Allow audio/video to play without a tap gesture (needed for received voice notes)
      androidController.setMediaPlaybackRequiresUserGesture(false);

      // 3. File & image picker — bridges <input type="file"> to native Android chooser.
      //    Without this the image/file buttons are silent no-ops inside WebView.
      androidController.setOnShowFileSelector(_handleFileSelector);

      // 4. Geolocation — enable in WebSettings and auto-grant permission prompt
      androidController.setGeolocationEnabled(true);
      androidController.setGeolocationPermissionsPromptCallbacks(
        onShowPrompt: (request) async {
          return const GeolocationPermissionsResponse(
            allow: true,
            retain: true,
          );
        },
        onHidePrompt: () {},
      );

      // 5. Allow file & content URI access so FileReader can read picked files/images
      androidController.setAllowFileAccess(true);
      androidController.setAllowContentAccess(true);
    }
  }

  /// Called by the WebView when the page invokes a file input (<input type="file">).
  /// Translates the web MIME/accept types into a native Android file picker and
  /// returns the selected file URI(s) back to the page.
  Future<List<String>> _handleFileSelector(FileSelectorParams params) async {
    try {
      FileType fileType = FileType.any;

      final accepts = params.acceptTypes.join(',').toLowerCase();
      if (accepts.contains('image/')) {
        fileType = FileType.image;
      } else if (accepts.contains('audio/') ||
          accepts.contains('.webm') ||
          accepts.contains('.mp3')) {
        fileType = FileType.audio;
      } else if (accepts.contains('video/')) {
        fileType = FileType.video;
      }

      List<PlatformFile>? result;
      try {
        result = await FilePicker.pickFiles(type: fileType);
      } catch (e) {
        debugPrint(
          '[LANMesh] Primary pickFiles failed, falling back to FileType.any: $e',
        );
        result = await FilePicker.pickFiles(type: FileType.any);
      }

      if (result.isNotEmpty) {
        return result
            .where((f) => f.path != null && f.path!.isNotEmpty)
            .map((f) => Uri.file(f.path!).toString())
            .toList();
      }
    } catch (e) {
      debugPrint('[LANMesh] File selector error: $e');
    }
    return [];
  }

  Future<void> _startPeerDaemon() async {
    try {
      await _platform.invokeMethod('startNode');
      _pollNodeHealth();
    } catch (e) {
      setState(() {
        _lastStatus = 'Failed to start engine: $e';
      });
    }
  }

  void _startStatusMonitoring() {
    _statusTimer = Timer.periodic(const Duration(seconds: 2), (_) async {
      try {
        final result = await _platform.invokeMethod<Map>('getNodeStatus');
        if (result != null && mounted) {
          setState(() {
            _isEngineRunning = result['isRunning'] as bool? ?? false;
            _peerId = result['peerId'] as String? ?? '';
            _port = result['port'] as int? ?? 8080;
            _lastStatus = result['lastStatus'] as String? ?? _lastStatus;
            _allLogs = result['logs'] as String? ?? _allLogs;
          });
        }
      } catch (_) {}
    });
  }

  void _pollNodeHealth() {
    _healthTimer?.cancel();
    _healthTimer = Timer.periodic(const Duration(milliseconds: 900), (
      timer,
    ) async {
      try {
        final res = await http
            .get(Uri.parse('http://127.0.0.1:$_port/'))
            .timeout(const Duration(milliseconds: 700));
        if (res.statusCode == 200 && mounted) {
          timer.cancel();
          setState(() {
            _isNodeReady = true;
          });
          _webViewController.loadRequest(Uri.parse('http://127.0.0.1:$_port/'));
        }
      } catch (_) {
        // Node still spinning up
      }
    });
  }

  void _showLogsModal() {
    showModalBottomSheet(
      context: context,
      backgroundColor: const Color(0xFF141414),
      shape: const RoundedRectangleBorder(
        borderRadius: BorderRadius.vertical(top: Radius.circular(16)),
        side: BorderSide(color: Color(0xFF2A2A2A)),
      ),
      builder: (ctx) {
        return Padding(
          padding: const EdgeInsets.all(20.0),
          child: Column(
            mainAxisSize: MainAxisSize.min,
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Row(
                children: [
                  const Icon(
                    Icons.terminal,
                    color: Color(0xFFF2C230),
                    size: 20,
                  ),
                  const SizedBox(width: 8),
                  const Text(
                    'Node Daemon Status',
                    style: TextStyle(
                      fontSize: 16,
                      fontWeight: FontWeight.bold,
                      color: Color(0xFFEEEEEA),
                    ),
                  ),
                  const Spacer(),
                  IconButton(
                    icon: const Icon(Icons.close, color: Color(0xFF8D8D87)),
                    onPressed: () => Navigator.pop(ctx),
                  ),
                ],
              ),
              const Divider(color: Color(0xFF2A2A2A)),
              const SizedBox(height: 8),
              _buildInfoRow(
                'Engine Status',
                _isEngineRunning
                    ? 'RUNNING (Foreground Service)'
                    : 'STARTING...',
              ),
              _buildInfoRow('Local Port', '$_port'),
              _buildInfoRow('MulticastLock', 'HELD (mDNS 224.0.0.251:5353)'),
              _buildInfoRow('WakeLock', 'PARTIAL_WAKE_LOCK ACTIVE'),
              if (_peerId.isNotEmpty)
                _buildInfoRow('libp2p Peer ID', _peerId, copyable: true),
              const SizedBox(height: 12),
              const Text(
                'Recent Daemon Log:',
                style: TextStyle(color: Color(0xFF8D8D87), fontSize: 12),
              ),
              const SizedBox(height: 6),
              Container(
                width: double.infinity,
                constraints: const BoxConstraints(maxHeight: 180),
                padding: const EdgeInsets.all(12),
                decoration: BoxDecoration(
                  color: const Color(0xFF0B0B0C),
                  borderRadius: BorderRadius.circular(8),
                  border: Border.all(color: const Color(0xFF2A2A2A)),
                ),
                child: SingleChildScrollView(
                  reverse: true,
                  child: Text(
                    _allLogs.isNotEmpty ? _allLogs : _lastStatus,
                    style: const TextStyle(
                      fontFamily: 'monospace',
                      fontSize: 11,
                      color: Color(0xFF3DDC84),
                    ),
                  ),
                ),
              ),
              const SizedBox(height: 16),
            ],
          ),
        );
      },
    );
  }

  Widget _buildInfoRow(String label, String value, {bool copyable = false}) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 4.0),
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          SizedBox(
            width: 120,
            child: Text(
              label,
              style: const TextStyle(color: Color(0xFF8D8D87), fontSize: 13),
            ),
          ),
          Expanded(
            child: Text(
              value,
              style: const TextStyle(
                color: Color(0xFFEEEEEA),
                fontSize: 13,
                fontWeight: FontWeight.w500,
              ),
            ),
          ),
          if (copyable)
            GestureDetector(
              onTap: () {
                Clipboard.setData(ClipboardData(text: value));
                ScaffoldMessenger.of(context).showSnackBar(
                  const SnackBar(
                    content: Text('Peer ID copied to clipboard'),
                    duration: Duration(seconds: 1),
                  ),
                );
              },
              child: const Icon(Icons.copy, size: 16, color: Color(0xFFF2C230)),
            ),
        ],
      ),
    );
  }

  @override
  void dispose() {
    _statusTimer?.cancel();
    _healthTimer?.cancel();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      body: SafeArea(
        child: Column(
          children: [
            // Sleek Mobile Top App Bar
            Container(
              padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 8),
              decoration: const BoxDecoration(
                color: Color(0xFF141414),
                border: Border(bottom: BorderSide(color: Color(0xFF222222))),
              ),
              child: Row(
                children: [
                  RichText(
                    text: const TextSpan(
                      style: TextStyle(
                        fontSize: 16,
                        fontWeight: FontWeight.w800,
                        letterSpacing: 0.5,
                      ),
                      children: [
                        TextSpan(
                          text: 'LAN',
                          style: TextStyle(color: Color(0xFFEEEEEA)),
                        ),
                        TextSpan(
                          text: 'MESH',
                          style: TextStyle(color: Color(0xFFF2C230)),
                        ),
                      ],
                    ),
                  ),
                  const SizedBox(width: 10),
                  // Live Status Pill (minimal and clean)
                  Container(
                    padding: const EdgeInsets.symmetric(
                      horizontal: 7,
                      vertical: 3,
                    ),
                    decoration: BoxDecoration(
                      color: _isNodeReady
                          ? const Color(0x1F3DDC84)
                          : const Color(0x1FF2C230),
                      borderRadius: BorderRadius.circular(16),
                      border: Border.all(
                        color: _isNodeReady
                            ? const Color(0x443DDC84)
                            : const Color(0x44F2C230),
                      ),
                    ),
                    child: Row(
                      mainAxisSize: MainAxisSize.min,
                      children: [
                        Container(
                          width: 5,
                          height: 5,
                          decoration: BoxDecoration(
                            shape: BoxShape.circle,
                            color: _isNodeReady
                                ? const Color(0xFF3DDC84)
                                : const Color(0xFFF2C230),
                          ),
                        ),
                        const SizedBox(width: 4),
                        Text(
                          _isNodeReady ? 'ONLINE' : 'CONNECTING',
                          style: TextStyle(
                            fontSize: 9,
                            fontWeight: FontWeight.w700,
                            letterSpacing: 0.5,
                            color: _isNodeReady
                                ? const Color(0xFF3DDC84)
                                : const Color(0xFFF2C230),
                          ),
                        ),
                      ],
                    ),
                  ),
                  const Spacer(),
                  // Action buttons
                  IconButton(
                    icon: const Icon(
                      Icons.info_outline,
                      color: Color(0xFF8D8D87),
                      size: 18,
                    ),
                    padding: const EdgeInsets.all(6),
                    constraints: const BoxConstraints(),
                    tooltip: 'Node Status',
                    onPressed: _showLogsModal,
                  ),
                  const SizedBox(width: 8),
                  IconButton(
                    icon: const Icon(
                      Icons.refresh,
                      color: Color(0xFF8D8D87),
                      size: 18,
                    ),
                    padding: const EdgeInsets.all(6),
                    constraints: const BoxConstraints(),
                    tooltip: 'Reload Chat',
                    onPressed: () {
                      if (_isNodeReady) {
                        _webViewController.reload();
                      } else {
                        _pollNodeHealth();
                      }
                    },
                  ),
                ],
              ),
            ),

            // Content Area
            Expanded(
              child: Stack(
                children: [
                  // Fullscreen WebView
                  if (_isNodeReady)
                    RefreshIndicator(
                      color: const Color(0xFFF2C230),
                      backgroundColor: const Color(0xFF141414),
                      onRefresh: () async {
                        _webViewController.reload();
                      },
                      child: WebViewWidget(controller: _webViewController),
                    ),

                  // Loading / Booting Overlay
                  if (!_isNodeReady)
                    Center(
                      child: Padding(
                        padding: const EdgeInsets.all(32.0),
                        child: Column(
                          mainAxisAlignment: MainAxisAlignment.center,
                          children: [
                            Container(
                              padding: const EdgeInsets.all(20),
                              decoration: BoxDecoration(
                                shape: BoxShape.circle,
                                color: const Color(0xFF141414),
                                border: Border.all(
                                  color: const Color(0xFF2A2A2A),
                                ),
                              ),
                              child: const Icon(
                                Icons.hub_rounded,
                                size: 48,
                                color: Color(0xFFF2C230),
                              ),
                            ),
                            const SizedBox(height: 24),
                            const SizedBox(
                              width: 24,
                              height: 24,
                              child: CircularProgressIndicator(
                                strokeWidth: 2.5,
                                valueColor: AlwaysStoppedAnimation<Color>(
                                  Color(0xFFF2C230),
                                ),
                              ),
                            ),
                            const SizedBox(height: 20),
                            const Text(
                              'Initializing LANMesh Peer Node',
                              style: TextStyle(
                                fontSize: 16,
                                fontWeight: FontWeight.bold,
                                color: Color(0xFFEEEEEA),
                              ),
                            ),
                            const SizedBox(height: 8),
                            const Text(
                              'Starting libp2p host, mDNS discovery & local BoltDB',
                              textAlign: TextAlign.center,
                              style: TextStyle(
                                fontSize: 12,
                                color: Color(0xFF8D8D87),
                              ),
                            ),
                            const SizedBox(height: 20),
                            TextButton.icon(
                              style: TextButton.styleFrom(
                                foregroundColor: const Color(0xFFF2C230),
                              ),
                              icon: const Icon(Icons.refresh, size: 16),
                              label: const Text('Retry Connection'),
                              onPressed: () {
                                _startPeerDaemon();
                                _pollNodeHealth();
                              },
                            ),
                          ],
                        ),
                      ),
                    ),
                ],
              ),
            ),
          ],
        ),
      ),
    );
  }
}
