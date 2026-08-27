"""
Package Tracking Event Generator
Generate synthetics scan events for supply chain tracking data pipeline testing.
No external dependencies
"""

import random 
import time 
import uuid
from datetime import datetime, timedelta
from typing import Dict,List,Any ,Optional,Iterator,Tuple,Generator
from dataclasses import dataclass, asdict
from enum import Enum
from collections import defaultdict


#Configuration and constants 

#Location pool for different scanner types 
WAREHOUSE_LOCATIONS:list=[
    "MEM-WH-01", "ATL-WH-03", "LAX-WH-02", "CHI-WH-01", "DFW-WH-02",
    "DEN-WH-01", "PHX-WH-03", "SEA-WH-01", "HOU-WH-01", "NYC-WH-03",
    "SFO-WH-02", "ORD-WH-01", "MSP-WH-02", "STL-WH-01", "PDX-WH-03",
    "RDU-WH-01", "BOS-WH-02", "CLE-WH-01", "SLC-WH-02", "IND-WH-01",
    "AUS-WH-03", "CMH-WH-02"
]

HUB_LOCATIONS:list=[
    "MEM-HUB-01", "MEM-HUB-03", "ATL-HUB-02", "LAX-HUB-01", "CHI-HUB-02",
    "DFW-HUB-03", "DEN-HUB-01", "PHX-HUB-01", "SEA-HUB-02", "HOU-HUB-02",
    "NYC-HUB-01", "SFO-HUB-03", "ORD-HUB-02", "MSP-HUB-01", "STL-HUB-02",
    "PDX-HUB-02", "RDU-HUB-01", "BOS-HUB-03", "CLE-HUB-02", "SLC-HUB-01",
    "IND-HUB-03", "AUS-HUB-02", "CMH-HUB-02"
]

DRIVER_ROUTES=[f"TRUCK-ROUTE-{i:02d}" for i in range(1,70)]
DELIVERY_ZONES=[f"RESIDENTIAL-ZONE-{chr(65+i)}" for i in range(26)]

#scan type definations
SCAN_TYPES = ["picked", "shipped", "in_transit", "out_for_delivery", "delivered"]
SCAN_STATE_INDEX = {
    stype:idx for idx , stype in enumerate(SCAN_TYPES)
}

#Default scanner configurations 
DEFAULT_SCANNER_CONFIG={
    "warehouse": {
        "prefix": "WH-SCANNER",
        "location_pool": WAREHOUSE_LOCATIONS,
        "offline_probability": 0.02,  # 2% offline
        "scan_types": ["picked"],
        "max_devices": 30
    },
    "hub": {
        "prefix": "HUB-SCANNER",
        "location_pool": HUB_LOCATIONS,
        "offline_probability": 0.08,  # 8% offline
        "scan_types": ["shipped", "in_transit"],
        "max_devices": 25
    },
    "driver": {
        "prefix": "TRUCK-SCANNER",
        "location_pool": DRIVER_ROUTES,
        "offline_probability": 0.20,  # 20% offline (dead zones!)
        "scan_types": ["in_transit", "out_for_delivery"],
        "max_devices": 70
    },
    "delivery": {
        "prefix": "DELIVERY-SCANNER",
        "location_pool": DELIVERY_ZONES,
        "offline_probability": 0.05,  # 5% offline
        "scan_types": ["delivered"],
        "max_devices": 60
    }
}

#Data Models 

@dataclass
class ScanEvent:
    """
    Represents a scan event with both event time and availability time.
    
    Attributes:
        package_id: Unique package identifier (format: PKG-XXXXXXXX)
        scan_type: Type of scan (picked, shipped, in_transit, out_for_delivery, delivered)
        location_id: Location where scan occurred
        event_time: ISO 8601 timestamp when the scan actually happened
        available_time: ISO 8601 timestamp when the event became available 
                       (after any offline delay)
        device_id: Scanner device identifier
        is_malformed: Flag indicating if this event contains deliberate data quality issues
        malformed_type: Type of malformation if is_malformed is True
    """
    package_id:str
    scan_type:str
    location_id:str
    event_time:str
    available_time:str
    device_id:str
    is_malformed:bool=False
    malformed_type:Optional[str]=None

    def to_dict(self) ->Dict:
        """
        Convert to dictionary for serialization
        """

        return asdict(self)

    def to_json(self)-> str:
        """
        Convert to JSON string
        """
        import json
        return json.dumps(self.to_dict())

    def is_valid(self)-> bool:
        """
        Check if the event meets expected data quality standards 
        Return False for malformed events
        """

        return not self.is_malformed

@dataclass
class Package:
    """
    Represents a package in the tracking system with its state machine progess.
    """

    package_id: str
    current_state_index:int # 0=picked, 1=shipped, 2=in_transit, 3=out_for_delivery, 4=delivered

    created_at :datetime
    last_event_time:datetime
    is_complete:bool=False

    def get_next_scan_type(self)-> Optional[str]:
        """
        Get te next valid scan type for thi package's current state
        """

        if self.current_state_index >= len(SCAN_TYPES):
            return None 

        return SCAN_TYPES[self.current_state_index]

    def advance_state(self)->bool:
        """
        Qdvance to the next state.
        Returns True if thepackage is now complete, False Otherwise
        """

        if self.current_state_index < len(SCAN_TYPES) -1:
            self.current_state_index +=1
            return True
        return False 

    def get_state_name(self)-> str:
        """
        Get the human-readable current state name
        """
        if self.current_state_index < len(SCAN_TYPES):
            return SCAN_TYPES[self.current_state_index]
        return "delivered"


#Scanner Device Simulator

class scannerDevice:
    """
    Simulate a physical scanner device with offline behavior
    """

    def __init__(self,
                 scanner_type:str,
                 device_id:str,
                 location_id:str,
                 offline_probability:float=0.05):
        """
        Initialize a scanner device.
        
        Args:
            scanner_type: Type of scanner (warehouse, hub, driver, delivery)
            device_id: Unique device identifier
            location_id: Physical location of the scanner
            offline_probability: Probability of being offline for any given scan
        """

        self.scanner_type = scanner_type
        self.device_id= device_id
        self.location_id = location_id
        self.offline_probability=offline_probability
        self.is_offline=False
        self.buffer:List[ScanEvent]=[]
        self.total_scans=0
        self.offline_scans=0

    def generate_scan(self,package:Package)-> Optional[ScanEvent]:
        """
        Generate a scan event for a package.
        
        Args:
            package: Package to scan
            
        Returns:
            ScanEvent if successful, None if no valid scan type or offline simulation
        """

        next_scan_type=package.get_next_scan_type()

        #check if this scanner can produce this scan type 
        scanner_config= DEFAULT_SCANNER_CONFIG.get(self.scanner_type,{})
        valid_scans= scanner_config.get("scan_types",[])

        if next_scan_type is None or next_scan_type not in valid_scans:
            return None

        self.total_scans+=1

        #simulate offline behavior 
        if random.random() < self.offline_probability:
            self.is_offline=True
            self.offline_scans +=1

            #create event buffer it( simulating delayed upload)

            event= self._create_event(package,next_scan_type)
            self.buffer.append(event)
            return None
        else:
            self.is_offline=False
            return self._create_event(package,next_scan_type)

    def _create_event(self,package:Package,scan_type:str)->ScanEvent:
        #calculate realistic delay between scans 
        delay_seconds= self._get_scan_delay(scan_type,package)
        event_time= package.last_event_time+ timedelta(seconds=delay_seconds)

        #determine if event should be malformed
        is_malformed= random.random() < 0.03
        malformed_type=None

        package_id= package.package_id
        location_id=self.location_id
        scan_type_out= scan_type

        if is_malformed:
            if scan_type=="picked":
            #randomly choose a malformation type 
                error_choice = random.choice([
                "bad_prefix", "null_location" 
                ])
            else:
                error_choice = random.choice([
                    "bad_prefix","null_location","bad_scan_type"
                ])
            if error_choice=="bad_prefix":

                #replace hyphen with underscore or remove prefix 
                package_id = package_id.replace("PKG-","PKG_")
                malformed_type="bad-prefix"

            elif error_choice == "null_location":
                location_id=None
                malformed_type="null_location"

            else:
                scan_type_out="mis_scanned"
                malformed_type="bad_scan_type"

        #determine availabe time 
        #if this eventt was buffered, we add a delay
        available_delay= random.randint(300,7200) if self.is_offline else 0
        available_time = event_time+ timedelta(seconds=available_delay)

        return ScanEvent(
            package_id=package_id,
            scan_type=scan_type_out,
            location_id=location_id,
            event_time=event_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            available_time=available_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            device_id=self.device_id,
            is_malformed=is_malformed,
            malformed_type=malformed_type
        )

    def _get_scan_delay(self,scan_type:str,package:Package)-> int:
        """
        Get realistic delay between scans in seconds.
        
        Based on requirements:
        - picked → shipped: few hours
        - shipped → in_transit → out_for_delivery: half day to day
        - out_for_delivery → delivered: 1-6 hours
        """

        if scan_type == 'picked':
            return random.randint(0,600) #first scan 
        elif scan_type=='shipped':
            return random.randint(7200,14400) # 2-4 hours 
        elif scan_type=='in_transit':
            return random.randint(43200,86400) #12-24 hours 
        elif scan_type=='out_for_delivery':
            return random.randint(3600,21600) #1-6 hours

        return random.randint(300,3600)

    def flush_buffer(self) -> List[ScanEvent]:
        """
        Flush all buffered events (simulating reconnection after being offline).
        
        Returns:
            List of buffered ScanEvents
        """
        events = self.buffer.copy()
        self.buffer.clear()
        self.is_offline=False 
        return events 

    def get_status(self)->Dict:
        return {
            'device_id':self.device_id,
            "total_scans":self.total_scans,
            "offline_scans":self.offline_scans,
            "buffer_size":len(self.buffer),
            "is_offline":self.is_offline
        }

#EVENT GENERATOR 

class PackageEventGenerator:
    """
    Genetrate package tracking scan events with realistic behavior.
    """

    def __init__(self,
                 num_packages:int=40,
                 scanner_config:Optional[Dict]=None,
                 seed:Optional[int]=None):
        """
        Initialize the event generator.
        
        Args:
            num_packages: Number of packages to simulate
            scanner_config: Custom scanner configuration (optional)
            seed: Random seed for reproducibility
        """

        if seed is not None:
            random.seed(seed)

        self.num_packages=num_packages
        self.scanner_config=scanner_config or DEFAULT_SCANNER_CONFIG

        #State 
        self.packages:Dict[str,Package]={}
        self.active_packages:List[Package]=[]
        self.completed_packages:List[Package]=[]
        self.scanners:Dict[str,scannerDevice]={}
        self.all_events:List[ScanEvent]=[]


        #initalize scanner 
        self._init_scanners()

        #initalize packages
        self._init_packages()

    def _init_scanners(self):
        scanners=[]

        for scanner_type,config in self.scanner_config.items():

            location_pool=config['location_pool']
            max_devices= config['max_devices']
            prefix=config['prefix']
            offline_prob=config['offline_probability']


            #Create devices 
            num_devices = min(max_devices, max(5, len(location_pool)*2))

            for i in range(num_devices):
                location = random.choice(location_pool)
                device_id=f"{prefix}-{i+1:02d}"

                scanner = scannerDevice(
                    scanner_type=scanner_type,
                    device_id=device_id,
                    location_id=location,
                    offline_probability=offline_prob
                )

                scanners.append(scanner)
                self.scanners[device_id]=scanner

            print(f"[Generator] Initialized {len(scanners)} scanner device")

    def _init_packages(self):

        #spread package creation over time 
        now = datetime.now()

        for i in range(self.num_packages):

            #generate random package id
            hex_part=''.join(random.choices('0123456789ABCDEF',k=8))
            pkg_id=f"PKG-{hex_part}"

            #stagger start time over the last 24-48 hours 
            start_hours_ago=random.uniform(0,48)
            start_time = now - timedelta(hours=start_hours_ago)

            package=Package(
                package_id=pkg_id,
                current_state_index=0,
                created_at=start_time,
                last_event_time=start_time,
                is_complete=False
            )

            self.packages[pkg_id]=package
            self.active_packages.append(package)

        print(f"[Generator] Created {self.num_packages} packages")

    def _get_scanner_for_packages(self, package: Package) -> Optional[scannerDevice]:
        """
        Get an appropriate scanner device for a package's next scan.
        
        Args:
            package: Package to scan
            
        Returns:
            ScannerDevice or None if no valid scanner available
        """
        next_scan = package.get_next_scan_type()
        if next_scan is None:
            return None
        
        # Find scanner type that can produce this scan
        for scanner_type, config in self.scanner_config.items():
            if next_scan in config["scan_types"]:
                # Get all scanners of this type
                available_scanners = [
                    s for s in self.scanners.values() 
                    if s.scanner_type == scanner_type
                ]
                if available_scanners:
                    return random.choice(available_scanners)
        
        return None
    def _get_scanner_for_state(self,scan_type: str,scanner_type_hint:Optional[str]=None) -> Optional[scannerDevice]:
        candidate= []
        for s in self.scanners.values():
            if scanner_type_hint and s.scanner_type != scanner_type_hint:
                continue
            config= DEFAULT_SCANNER_CONFIG.get(s.scanner_type,{})
            if scan_type in config.get("scan_types",[]):
                candidate.append(s)
        return random.choice(candidate) if candidate else None

    
    def _process_package_step(self, package: Package) -> Optional[ScanEvent]:
        if package.is_complete:
            return None

        next_scan = package.get_next_scan_type()
        if next_scan is None:
            return None

        # Enforce: First scan must be "picked" from warehouse
        if package.current_state_index == 0:
            scanner = self._get_scanner_for_state("picked", scanner_type_hint="warehouse")
        else:
            scanner = self._get_scanner_for_state(next_scan)

        if scanner is None:
            return None

        event = scanner.generate_scan(package)

        if event:
            # Validate first event
            if package.current_state_index == 0 and event.scan_type != "picked":
                raise RuntimeError(
                    f"Package {package.package_id}: first event was '{event.scan_type}', "
                    f"expected 'picked'"
                )

            event_time = datetime.strptime(event.event_time, "%Y-%m-%dT%H:%M:%SZ")
            package.last_event_time = event_time
            
            # If we can't, we're at "delivered" and done.
            advanced = package.advance_state()
            if not advanced:
                package.is_complete = True

            if package.is_complete and package in self.active_packages:
                self.active_packages.remove(package)
                self.completed_packages.append(package)

            return event

        return None

    def _process_buffered_events(self) -> List[ScanEvent]:
        all_buffered = []
        for scanner in self.scanners.values():
            buffered = scanner.flush_buffer()
            for event in buffered:
                pkg = self.packages.get(event.package_id)
                if pkg and not pkg.is_complete:
                    expected_next = pkg.get_next_scan_type()
                    if event.scan_type == expected_next:
                        event_time = datetime.strptime(
                            event.event_time, "%Y-%m-%dT%H:%M:%SZ"
                        )
                        pkg.last_event_time = event_time
                        
                        advanced = pkg.advance_state()
                        if not advanced:
                            pkg.is_complete = True
                        
                        if pkg.is_complete and pkg in self.active_packages:
                            self.active_packages.remove(pkg)
                            self.completed_packages.append(pkg)
                all_buffered.append(event)
        return all_buffered
    
    def generate_batch(self, max_events: Optional[int] = None) -> List[ScanEvent]:
        """
        Generate a fixed batch of events.
        
        Args:
            max_events: Maximum number of events to generate.
                    If None, generate until all packages are complete.
        
        Returns:
            List of ScanEvents
        """
        events = []
        attempts = 0
        max_attempts = max_events * 3 if max_events else self.num_packages * 50
        
        while (max_events is None or len(events) < max_events) and self.active_packages:
            # Process active packages
            for package in self.active_packages[:]:
                event = self._process_package_step(package)
                if event:
                    events.append(event)
                    
                    # Check if we have enough events
                    if max_events and len(events) >= max_events:
                        break
                
                # Randomly process buffered events
                if random.random() < 0.1 or not max_events:  # 10% chance per iteration
                    buffered = self._process_buffered_events()
                    events.extend(buffered)
            
            attempts += 1
            if attempts > max_attempts:
                break
        
        # Flush any remaining buffered events
        remaining = self._process_buffered_events()
        events.extend(remaining)
        
        # Store for statistics
        self.all_events.extend(events)
        
        return events
    
    def generate_stream(self, 
                        events_per_batch: int = 10,
                        delay_seconds: float = 0.5) -> Generator[ScanEvent, None, None]:
        """
        Generate a continuous stream of events.
        
        Args:
            events_per_batch: Number of events to yield per batch
            delay_seconds: Simulated delay between batches
            
        Yields:
            ScanEvent objects
        """
        while self.active_packages:
            batch_events = []
            
            # Process some packages
            for _ in range(events_per_batch):
                if not self.active_packages:
                    break
                
                # Randomly select a package to process
                package = random.choice(self.active_packages)
                event = self._process_package_step(package)
                
                if event:
                    batch_events.append(event)
                
                # Occasionally flush buffered events
                if random.random() < 0.05:  # 5% chance
                    buffered = self._process_buffered_events()
                    batch_events.extend(buffered)
            
            # Yield events from this batch
            for event in batch_events:
                yield event
                self.all_events.append(event)
            
            # Simulate time passing
            if delay_seconds > 0 and self.active_packages:
                time.sleep(delay_seconds)
        
        # Final flush of buffered events
        remaining = self._process_buffered_events()
        for event in remaining:
            yield event
            self.all_events.append(event)
    
    def get_statistics(self) -> Dict:
        """
        Get generator statistics.
        
        Returns:
            Dictionary with statistics about generated events
        """
        total_events = len(self.all_events)
        malformed = sum(1 for e in self.all_events if e.is_malformed)
        
        # Count by scanner type
        by_scanner_type = defaultdict(int)
        for event in self.all_events:
            # Find scanner type from device_id prefix
            device_prefix = event.device_id.split('-')[0] if '-' in event.device_id else 'unknown'
            by_scanner_type[device_prefix] += 1
        
        return {
            "total_events": total_events,
            "total_packages": self.num_packages,
            "completed_packages": len(self.completed_packages),
            "malformed_events": malformed,
            "malformed_percentage": (malformed / total_events * 100) if total_events > 0 else 0,
            "events_by_scanner_type": dict(by_scanner_type),
            "scanner_stats": {
                device_id: scanner.get_status() 
                for device_id, scanner in self.scanners.items()
            }
        }

#convenience Functions 

def generate_package_events(num_packages: int = 40,
                           max_events: Optional[int] = None,
                           seed: Optional[int] = None) -> List[ScanEvent]:
    """
    Convenience function to generate a batch of events.
    
    Args:
        num_packages: Number of packages to simulate
        max_events: Maximum number of events to generate
        seed: Random seed for reproducibility
        
    Returns:
        List of ScanEvents
    """
    generator = PackageEventGenerator(num_packages=num_packages, seed=seed)
    events = generator.generate_batch(max_events=max_events)
    return events


def stream_package_events(num_packages: int = 40,
                         events_per_batch: int = 5,
                         delay_seconds: float = 1.0,
                         seed: Optional[int] = None) -> Generator[ScanEvent, None, None]:
    """
    Convenience function to stream events continuously.
    
    Args:
        num_packages: Number of packages to simulate
        events_per_batch: Events to yield per batch
        delay_seconds: Delay between batches
        seed: Random seed for reproducibility
        
    Yields:
        ScanEvent objects
    """
    generator = PackageEventGenerator(num_packages=num_packages, seed=seed)
    yield from generator.generate_stream(
        events_per_batch=events_per_batch,
        delay_seconds=delay_seconds
    )
if __name__ == "__main__":
    import argparse
    import json
    
    parser = argparse.ArgumentParser(
        description="Generate synthetic package tracking scan events"
    )
    parser.add_argument(
        "--packages", "-p",
        type=int,
        default=40,
        help="Number of packages to simulate (default: 40)"
    )
    parser.add_argument(
        "--events", "-e",
        type=int,
        default=None,
        help="Number of events to generate (default: complete all packages)"
    )
    parser.add_argument(
        "--seed", "-s",
        type=int,
        default=None,
        help="Random seed for reproducibility"
    )
    parser.add_argument(
        "--stream", 
        action="store_true",
        help="Generate events as a continuous stream"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5,
        help="Events per batch when streaming (default: 5)"
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Delay between batches in seconds (default: 1.0)"
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default=None,
        help="Output file path (default: print to stdout)"
    )
    
    args = parser.parse_args()
    
    # Generate events
    if args.stream:
        print(f"Streaming events for {args.packages} packages...")
        events_generator = stream_package_events(
            num_packages=args.packages,
            events_per_batch=args.batch_size,
            delay_seconds=args.delay,
            seed=args.seed
        )
        
        # Collect events
        events = []
        try:
            for event in events_generator:
                events.append(event)
                print(f"Generated event: {event.event_time} | {event.package_id} | {event.scan_type}")
        except KeyboardInterrupt:
            print("\nStopped streaming...")
    else:
        print(f"Generating {args.events or 'all'} events for {args.packages} packages...")
        events = generate_package_events(
            num_packages=args.packages,
            max_events=args.events,
            seed=args.seed
        )
    
    # Output results
    output_data = [event.to_dict() for event in events]
    
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(output_data, f, indent=2)
        print(f"\nGenerated {len(events)} events, saved to {args.output}")
    else:
        # Print as JSON
        print("\n" + "=" * 80)
        print(f"Generated {len(events)} events")
        print("=" * 80)
        
        # Print first 5 events as sample
        for i, event in enumerate(events[:5]):
            print(f"\nEvent {i+1}:")
            print(f"  Package: {event.package_id}")
            print(f"  Scan: {event.scan_type}")
            print(f"  Location: {event.location_id}")
            print(f"  Event Time: {event.event_time}")
            print(f"  Available: {event.available_time}")
            print(f"  Device: {event.device_id}")
            if event.is_malformed:
                print(f"MALFORMED: {event.malformed_type}")
        
        if len(events) > 5:
            print(f"\n... and {len(events) - 5} more events")
        
        # Show statistics
        print("\n Statistics:")
        total = len(events)
        malformed = sum(1 for e in events if e.is_malformed)
        completed = sum(1 for e in events if e.scan_type == "delivered")
        
        print(f"  Total events: {total}")
        print(f"  Malformed: {malformed} ({malformed/total*100:.1f}%)" if total > 0 else "  Malformed: 0")
        print(f"  Completed deliveries: {completed}")
        
        # Show full JSON if asked
        if args.events and args.events <= 20:
            print("\n Full JSON output:")
            print(json.dumps(output_data, indent=2))