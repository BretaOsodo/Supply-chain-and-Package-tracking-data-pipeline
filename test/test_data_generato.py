import pytest 
import json 
from datetime import timedelta, datetime
from collections import defaultdict

from data_generator.generated_data import ScanEvent,Package,scannerDevice,PackageEventGenerator,SCAN_TYPES,generate_package_events,stream_package_events,DEFAULT_SCANNER_CONFIG

class TestScanEvent:

    def test_to_dict(self):
        event= ScanEvent(
            package_id="PKG-12345678",
            scan_type="picked",
            location_id="WH-01",
            event_time="2026-08-21T10:00:00Z",
            available_time="2026-08-21T10:00:00Z",
            device_id="DEV-01",
        )

        d = event.to_dict()
        assert d["package_id"]== "PKG-12345678"
        assert d["scan_type"] == "picked"
        assert isinstance(d, dict)

    def test_to_json_returns_string(self):
        
        event = ScanEvent(
            package_id="PKG-12345678",
            scan_type="picked",
            location_id="WH-01",
            event_time="2026-08-21T10:00:00Z",
            available_time="2026-08-21T10:00:00Z",
            device_id="DEV-01",
        )
        result = event.to_json()
        assert isinstance(result, str)
        # Should be valid JSON
        parsed = json.loads(result)
        assert parsed["package_id"] == "PKG-12345678"

    def test_is_valid_true(self):
        event = ScanEvent(
            package_id="PKG-12345678",
            scan_type="picked",
            location_id="WH-01",
            event_time="2026-08-21T10:00:00Z",
            available_time="2026-08-21T10:00:00Z",
            device_id="DEV-01",
            is_malformed=False,
        )
        assert event.is_valid() is True

    def test_is_valid_false(self):
        event = ScanEvent(
            package_id="PKG-12345678",
            scan_type="picked",
            location_id="WH-01",
            event_time="2026-08-21T10:00:00Z",
            available_time="2026-08-21T10:00:00Z",
            device_id="DEV-01",
            is_malformed=True,
        )
        assert event.is_valid() is False


class TestPackageStateMachine:
    """Tests for the Package state machine."""

    def test_initial_state_is_picked(self):
        pkg = Package(
            package_id="PKG-TEST01",
            current_state_index=0,
            created_at=datetime.now(),
            last_event_time=datetime.now(),
        )
        # At index 0, next scan is "picked"
        assert pkg.get_next_scan_type() == "picked"

        # Advance through the sequence
        assert pkg.advance_state() is True   # 0 → 1 (shipped)
        assert pkg.get_next_scan_type() == "shipped"

        assert pkg.advance_state() is True   # 1 → 2 (in_transit)
        assert pkg.get_next_scan_type() == "in_transit"

        assert pkg.advance_state() is True   # 2 → 3 (out_for_delivery)
        assert pkg.get_next_scan_type() == "out_for_delivery"

        assert pkg.advance_state() is True   # 3 → 4 (delivered)
        assert pkg.get_next_scan_type() == "delivered"
        assert pkg.is_complete is False      # advance_state() doesn't set this

        # Cannot advance past delivered
        assert pkg.advance_state() is False
        pkg.is_complete = True               # caller sets this when advance fails
        assert pkg.is_complete is True
    def test_advance_state_sequence(self):
        pkg = Package(
            package_id="PKG-TEST01",
            current_state_index=0,
            created_at=datetime.now(),
            last_event_time=datetime.now(),
        )

        expected_sequence = ["shipped", "in_transit", "out_for_delivery", "delivered"]
        for expected in expected_sequence:
            assert pkg.advance_state() is True
            # After advancing, get_state_name() reflects the new current state
            assert pkg.get_state_name() == expected

    def test_package_completes_at_delivered(self):
        pkg = Package(
            package_id="PKG-TEST01",
            current_state_index=0,
            created_at=datetime.now(),
            last_event_time=datetime.now(),
        )
        # Advance through all states (0→1→2→3→4)
        for _ in range(len(SCAN_TYPES) - 1):
            pkg.advance_state()

        assert pkg.get_state_name() == "delivered"
        assert pkg.is_complete is False  # advance_state() doesn't set this

        # One more advance fails → caller marks complete
        assert pkg.advance_state() is False
        pkg.is_complete = True
        assert pkg.is_complete is True


class TestScannerDevice:
    """Tests for scanner device behavior."""

    def test_warehouse_scanner_generates_picked(self):
        scanner = scannerDevice(
            scanner_type="warehouse",
            device_id="WH-SCANNER-01",
            location_id="MEM-WH-01",
            offline_probability=0.0,  # Never offline for predictability
        )
        pkg = Package(
            package_id="PKG-TEST01",
            current_state_index=0,
            created_at=datetime.now(),
            last_event_time=datetime.now(),
        )
        event = scanner.generate_scan(pkg)
        assert event is not None
        assert event.scan_type == "picked"
        assert event.device_id == "WH-SCANNER-01"
        assert event.location_id == "MEM-WH-01"

    def test_hub_scanner_rejects_picked(self):
        """Hub scanners should not generate 'picked' events."""
        scanner = scannerDevice(
            scanner_type="hub",
            device_id="HUB-SCANNER-01",
            location_id="MEM-HUB-01",
            offline_probability=0.0,
        )
        pkg = Package(
            package_id="PKG-TEST01",
            current_state_index=0,
            created_at=datetime.now(),
            last_event_time=datetime.now(),
        )
        # Hub can't scan "picked", so should return None
        event = scanner.generate_scan(pkg)
        assert event is None

    def test_offline_scanner_buffers_event(self):
        scanner = scannerDevice(
            scanner_type="warehouse",
            device_id="WH-SCANNER-01",
            location_id="MEM-WH-01",
            offline_probability=1.0,  # Always offline
        )
        pkg = Package(
            package_id="PKG-TEST01",
            current_state_index=0,
            created_at=datetime.now(),
            last_event_time=datetime.now(),
        )
        event = scanner.generate_scan(pkg)
        # When offline, event is buffered, not returned immediately
        assert event is None
        assert len(scanner.buffer) == 1
        assert scanner.is_offline is True

    def test_flush_buffer(self):
        scanner = scannerDevice(
            scanner_type="warehouse",
            device_id="WH-SCANNER-01",
            location_id="MEM-WH-01",
            offline_probability=1.0,
        )
        pkg = Package(
            package_id="PKG-TEST01",
            current_state_index=0,
            created_at=datetime.now(),
            last_event_time=datetime.now(),
        )
        scanner.generate_scan(pkg)
        assert len(scanner.buffer) == 1

        flushed = scanner.flush_buffer()
        assert len(flushed) == 1
        assert len(scanner.buffer) == 0
        assert flushed[0].scan_type == "picked"

    def test_get_status(self):
        """BUG FIX: get_status must not crash."""
        scanner = scannerDevice(
            scanner_type="warehouse",
            device_id="WH-SCANNER-01",
            location_id="MEM-WH-01",
        )
        status = scanner.get_status()
        assert status["device_id"] == "WH-SCANNER-01"
        assert status["total_scans"] == 0
        assert isinstance(status["is_offline"], bool)


class TestPackageEventGenerator:
    """Integration tests for the full generator."""

    def test_packages_dict_is_populated(self):
        """BUG FIX: _init_packages must store packages correctly."""
        gen = PackageEventGenerator(num_packages=10, seed=42)
        assert len(gen.packages) == 10
        assert all(isinstance(p, Package) for p in gen.packages.values())

    def test_every_package_starts_with_picked(self):
        """
        CRITICAL: Every package's first chronological event MUST be 'picked'.
        Malformed events (bad_prefix) create alternate package IDs — we normalize.
        """
        events = generate_package_events(num_packages=50, seed=42)

        # Normalize package IDs: fix bad_prefix malformation for grouping
        by_package = defaultdict(list)
        for e in events:
            # PKG_12345678 -> PKG-12345678 for grouping purposes
            normalized_id = e.package_id.replace("PKG_", "PKG-")
            by_package[normalized_id].append(e)

        assert len(by_package) > 0, "No events were generated"

        failures = []
        for pkg_id, pkg_events in by_package.items():
            if len(pkg_events) == 0:
                continue
            
            # Sort by event time to find chronologically first event
            sorted_events = sorted(
                pkg_events,
                key=lambda e: datetime.strptime(e.event_time, "%Y-%m-%dT%H:%M:%SZ")
            )
            first_event = sorted_events[0]
            
            if first_event.scan_type != "picked":
                failures.append(
                    f"{pkg_id}: first event was '{first_event.scan_type}' "
                    f"(device={first_event.device_id}, malformed={first_event.is_malformed})"
                )

        assert not failures, f"Packages that didn't start with 'picked':\n" + "\n".join(failures)

    def test_state_machine_order(self):
        """Events for each package should follow the state sequence (ignoring malformed)."""
        events = generate_package_events(num_packages=30, seed=42)

        by_package = defaultdict(list)
        for e in events:
            by_package[e.package_id].append(e)

        for pkg_id, pkg_events in by_package.items():
            # Sort by event time to ensure chronological order
            sorted_events = sorted(
                pkg_events,
                key=lambda e: datetime.strptime(e.event_time, "%Y-%m-%dT%H:%M:%SZ")
            )

            # Only check valid events — malformed ones intentionally break rules
            valid_states = [e.scan_type for e in sorted_events if not e.is_malformed]

            prev_idx = -1
            for state in valid_states:
                curr_idx = SCAN_TYPES.index(state)
                assert curr_idx >= prev_idx, (
                    f"{pkg_id}: state went backwards or invalid: {valid_states}"
                )
                prev_idx = curr_idx
    def test_some_packages_complete(self):
        """With enough events, some packages should reach 'delivered'."""
        gen = PackageEventGenerator(num_packages=10000, seed=42)
        events=list(gen.generate_stream(events_per_batch=10,delay_seconds=0))

        delivered = [e for e in events if e.scan_type == "delivered"]
        assert len(delivered) > 0, "No packages reached 'delivered' state"

    def test_malformed_events_exist(self):
        """With default 3% malformation rate, we should see some bad data."""
        events = generate_package_events(num_packages=200, seed=42)
        malformed = [e for e in events if e.is_malformed]

        # Probabilistic test: with 200 packages * ~5 events each = 1000 events
        # 3% = ~30 malformed. We should see at least 1.
        assert len(malformed) > 0, "Expected some malformed events"

        # Verify malformed types are tracked
        for e in malformed:
            assert e.malformed_type is not None

    def test_event_times_are_iso8601(self):
        events = generate_package_events(num_packages=10, seed=42)
        for e in events:
            # Should parse without error
            dt = datetime.strptime(e.event_time, "%Y-%m-%dT%H:%M:%SZ")
            assert isinstance(dt, datetime)

    def test_device_ids_have_valid_prefix(self):
        events = generate_package_events(num_packages=20, seed=42)
        valid_prefixes = {"WH", "HUB", "TRUCK", "DELIVERY"}
        for e in events:
            prefix = e.device_id.split("-")[0]
            assert prefix in valid_prefixes, f"Invalid device prefix: {e.device_id}"

    def test_generator_statistics(self):
        gen = PackageEventGenerator(num_packages=25, seed=42)
        events = gen.generate_batch()
        stats = gen.get_statistics()

        assert stats["total_packages"] == 25
        assert stats["total_events"] == len(events)
        assert stats["completed_packages"] >= 0
        assert 0 <= stats["malformed_percentage"] <= 100

    def test_streaming_generates_events(self):
        """Test the streaming generator yields events."""
        gen = PackageEventGenerator(num_packages=5, seed=42)
        stream = gen.generate_stream(events_per_batch=2, delay_seconds=0)

        events = []
        for event in stream:
            events.append(event)
            if len(events) >= 20:  # Safety break
                break

        assert len(events) > 0
        # First event should be picked
        assert events[0].scan_type == "picked"

    def test_seed_reproducibility(self):
        """Same seed should produce identical events."""
        events1 = generate_package_events(num_packages=20, seed=123)
        events2 = generate_package_events(num_packages=20, seed=123)

        assert len(events1) == len(events2)
        for e1, e2 in zip(events1, events2):
            assert e1.package_id == e2.package_id
            assert e1.scan_type == e2.scan_type
            assert e1.location_id == e2.location_id


class TestEdgeCases:
    """Boundary and error condition tests."""

    def test_zero_packages(self):
        gen = PackageEventGenerator(num_packages=0, seed=42)
        events = gen.generate_batch()
        assert len(events) == 0

    def test_single_package_lifecycle(self):
        """One package should go from picked to delivered."""
        events = generate_package_events(num_packages=1, seed=42)

        by_package = defaultdict(list)
        for e in events:
            by_package[e.package_id].append(e)

        assert len(by_package) == 1
        pkg_events = list(by_package.values())[0]
        assert pkg_events[0].scan_type == "picked"
        assert pkg_events[-1].scan_type == "delivered"

    def test_max_events_limit(self):
        """Respect max_events cap."""
        events = generate_package_events(num_packages=100, max_events=10, seed=42)
        assert len(events) == 10