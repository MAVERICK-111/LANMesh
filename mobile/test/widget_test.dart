import 'package:flutter_test/flutter_test.dart';
import 'package:lanmesh_mobile/main.dart';

void main() {
  testWidgets('App smoke test', (WidgetTester tester) async {
    await tester.pumpWidget(const LanMeshApp());
    expect(find.text('LAN'), findsOneWidget);
  });
}
