from data_generator.generated_data import stream_package_events

# Process events directly in the loop
for event in stream_package_events(
    num_packages=50,
    events_per_batch=5,
    delay_seconds=2.0
):
    # Do something with the event right here
    print(f"📦 {event.package_id} → {event.scan_type} at {event.event_time}")
    if event.is_malformed:
        print(f"   ⚠️ Malformed: {event.malformed_type}")
    
    # Example: Send to Kafka
    # producer.send('package-scans', value=event.to_dict())