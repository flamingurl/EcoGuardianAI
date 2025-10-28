import pandas as pd
from datetime import datetime, timedelta
import time
import random

#  1. Define Lab Safety Windows and Severity Scale 

# Severity Scale definition
SEVERITY_SCALE = {
    0: "Info (Normal)",
    1: "Warning",
    2: "Critical"
}

# Thresholds for Monitoring (Safe Lab Windows)
LAB_WINDOWS = {
    # Ideal CO2: 400-1000 ppm
    'co2': {'warning_high': 1000, 'critical_high': 1500, 'unit': 'ppm'},
    # Ideal Humidity: 30-60%
    'humidity': {'warning_low': 30, 'warning_high': 60, 'unit': '%'},
    # Ideal Temperature: 20-24°C
    'temperature': {'warning_low': 20, 'warning_high': 24, 'unit': '°C'},
    # PM2.5 Good Air Quality: <= 12 μg/m³. Critical threshold set higher.
    'pm2_5': {'warning_high': 12, 'critical_high': 35, 'unit': 'μg/m³'}
}

#  2. Anomaly Generation Function 

def generate_incident_report(row, field, severity, reason, window_end_dt: datetime):
    """
    Generates a structured incident report record (anomaly record).
    The window_end_dt must be a standard Python datetime object.
    """

    window_end = window_end_dt

    # Define incident window (e.g., last 10 seconds)
    window_start = window_end - timedelta(seconds=10)

    # Use the reading's source ID for context
    anomaly_id = f"ANOMALY-{hash(str(row)) % 10000}-{window_end.strftime('%H%M%S')}"

    # Helper function to format datetime to standard ISO 8601 (local time, no Z suffix)
    def format_timestamp(dt):
        # We use a simple ISO format string without the Z or explicit timezone offset
        return dt.isoformat() 

    # IMPORTANT: The key names below (id, roomId, timestamp, etc.) MUST NOT be changed per user request.
    return {
        "id": anomaly_id,
        "roomId": row['roomId'],
        "timestamp": format_timestamp(window_end), # Updated to use local time format
        "measurementType": field,
        "value": row[field],
        "qualityFlags": f"{field.upper()}_OUT_OF_RANGE",
        "reason": reason,
        "severity": severity,
        "severityText": SEVERITY_SCALE.get(severity, "Unknown"),
        "ackBy": None,
        "ackTs": None,
        "windowStart": format_timestamp(window_start), # Updated to use local time format
        "windowEnd": format_timestamp(window_end) # Updated to use local time format
    }

#  3. Main Analysis Function (Anomaly Agent Core) 

def evaluate_sensor_data_for_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    anomalies = []

    # timestamp fix
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    for _, row in df.iterrows():
        # Convert the Pandas Timestamp object to a Python datetime object
        current_dt = row['timestamp'].to_pydatetime()

        #  CO2 Monitoring 
        co2 = row['co2']
        if co2 > LAB_WINDOWS['co2']['critical_high']:
            reason = f"CO2 ({co2:.0f} ppm) exceeded critical limit of {LAB_WINDOWS['co2']['critical_high']} ppm."
            anomalies.append(generate_incident_report(row, 'co2', 2, reason, current_dt))
        elif co2 > LAB_WINDOWS['co2']['warning_high']:
            reason = f"CO2 ({co2:.0f} ppm) exceeded warning limit of {LAB_WINDOWS['co2']['warning_high']} ppm."
            anomalies.append(generate_incident_report(row, 'co2', 1, reason, current_dt))

        #  Humidity Monitoring 
        humidity = row['humidity']
        if humidity < LAB_WINDOWS['humidity']['warning_low']:
            reason = f"Humidity ({humidity:.1f} %) dropped below low limit of {LAB_WINDOWS['humidity']['warning_low']}%."
            anomalies.append(generate_incident_report(row, 'humidity', 1, reason, current_dt))
        elif humidity > LAB_WINDOWS['humidity']['warning_high']:
            reason = f"Humidity ({humidity:.1f} %) exceeded high limit of {LAB_WINDOWS['humidity']['warning_high']}%."
            anomalies.append(generate_incident_report(row, 'humidity', 1, reason, current_dt))

        #  Temperature Monitoring 
        temperature = row['temperature']
        if temperature < LAB_WINDOWS['temperature']['warning_low']:
            reason = f"Temperature ({temperature:.1f}°C) dropped below low limit of {LAB_WINDOWS['temperature']['warning_low']}°C."
            anomalies.append(generate_incident_report(row, 'temperature', 1, reason, current_dt))
        elif temperature > LAB_WINDOWS['temperature']['warning_high']:
            reason = f"Temperature ({temperature:.1f}°C) exceeded high limit of {LAB_WINDOWS['temperature']['warning_high']}°C."
            anomalies.append(generate_incident_report(row, 'temperature', 1, reason, current_dt))

        #  PM2.5 Monitoring 
        pm2_5 = row['pm2_5']
        if pm2_5 > LAB_WINDOWS['pm2_5']['critical_high']:
            reason = f"PM2.5 ({pm2_5:.1f} μg/m³) exceeded critical limit of {LAB_WINDOWS['pm2_5']['critical_high']} μg/m³."
            anomalies.append(generate_incident_report(row, 'pm2_5', 2, reason, current_dt))
        elif pm2_5 > LAB_WINDOWS['pm2_5']['warning_high']:
            reason = f"PM2.5 ({pm2_5:.1f} μg/m³) exceeded warning limit of {LAB_WINDOWS['pm2_5']['warning_high']} μg/m³."
            anomalies.append(generate_incident_report(row, 'pm2_5', 1, reason, current_dt))

    # Create the Anomaly Report DataFrame
    if not anomalies:
        return pd.DataFrame()
    return pd.DataFrame(anomalies)

#  4. Real-time Simulation and Execution 

# Base readings for simulation, stored as a dictionary
BASE_READINGS = {
    'Lab-A-205': {'co2': 600, 'humidity': 45.0, 'temperature': 21.5, 'pm2_5': 7.0},
    'Lab-B-101': {'co2': 750, 'humidity': 40.0, 'temperature': 22.0, 'pm2_5': 9.0},
    'Lab-C-302': {'co2': 680, 'humidity': 50.0, 'temperature': 20.5, 'pm2_5': 6.5},
}

# List of room IDs to cycle through
ROOM_IDS = list(BASE_READINGS.keys())
record_id_counter = 1000

def simulate_sensor_reading(room_id, counter):
    """Generates a new simulated sensor reading with random and intentional fluctuations."""
    global record_id_counter
    base = BASE_READINGS[room_id]

    # Random fluctuation (standard deviation)
    std_dev = {'co2': 50, 'humidity': 1.5, 'temperature': 0.5, 'pm2_5': 1.0}

    new_reading = {
        'id': counter,
        'roomId': room_id,
        'timestamp': datetime.now().isoformat(), # Removed 'Z' for local time
        'co2': max(0, base['co2'] + random.gauss(0, std_dev['co2'])),
        'humidity': max(0, min(100, base['humidity'] + random.gauss(0, std_dev['humidity']))),
        'temperature': base['temperature'] + random.gauss(0, std_dev['temperature']),
        'pm2_5': max(0, base['pm2_5'] + random.gauss(0, std_dev['pm2_5']))
    }

    # Introduce a specific anomaly roughly every 15 readings, tests each parameter
    if counter % 15 == 0:
        if room_id == 'Lab-B-101':
            new_reading['co2'] = 1650 + random.randint(0, 100) # Critical CO2 spike
            new_reading['pm2_5'] = 45.0 + random.randint(0, 10) # Critical PM2.5 spike
        elif room_id == 'Lab-C-302':
            new_reading['temperature'] = 26.0 + random.uniform(0, 1) # High Temperature
            new_reading['humidity'] = 20.0 - random.uniform(0, 5) # Low Humidity

    record_id_counter = counter + 1
    return new_reading

def start_realtime_monitoring_loop():
    print("Lab Air Quality Monitoring Started")
    print("Monitoring simulated data stream. Press Ctrl+C to stop.\n")

    current_room_index = 0
    reading_count = 1

    try:
        while True:
            room_id = ROOM_IDS[current_room_index % len(ROOM_IDS)]

            # 1. Generate new reading for the next room
            new_data_row = simulate_sensor_reading(room_id, reading_count)

            # 2. Convert the single row to a DataFrame for analysis
            df_new_reading = pd.DataFrame([new_data_row])

            # 3. Run anomaly check on the latest reading
            anomaly_df = evaluate_sensor_data_for_anomalies(df_new_reading)
            
            # 4. Output results
            # The timestamp is sliced to only show HH:MM:SS for cleaner console output
            time_slice = new_data_row['timestamp'][11:19] 
            
            if not anomaly_df.empty:
                print("\n" + "="*80)
                print(f"!!! ANOMALY DETECTED at {room_id} ({time_slice} Local Time) !!!")
                print("="*80)
                # Using to_string() for console output
                print(anomaly_df[['roomId', 'timestamp', 'measurementType', 'value', 'reason', 'severityText', 'windowStart']].to_string(index=False))
                print("="*80 + "\n")
            else:
                # Print a status update to show the system is running
                if reading_count % 5 == 0:
                    print(f"[{time_slice} Local Time] Reading #{reading_count}: {room_id} - All OK (CO2: {new_data_row['co2']:.0f} ppm)")

            # 5. Prepare for next iteration
            current_room_index += 1 # Iterate to next room
            reading_count += 1 # Increase read count

            # 6. Sleep for a pseudo-random interval (5 to 15 seconds) - change if needed
            sleep_time = random.uniform(5, 15)
            # Clarified the output to confirm the sleep is in SECONDS
            if reading_count % 5 == 1: # Print this every time after a status update or anomaly
                print(f"INFO: Next check in {sleep_time:.1f} seconds...")
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\n\n--- Monitoring Interrupted by User (Ctrl+C). Shutting down. ---")

if __name__ == "__main__":
    start_realtime_monitoring_loop()
