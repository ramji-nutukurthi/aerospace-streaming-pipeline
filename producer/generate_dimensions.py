import os
import json
import random
from datetime import datetime, timedelta

def setup_local_folders():
    dimensions = ['dim_engines', 'dim_thresholds', 'dim_aircraft', 'dim_locations', 'dim_parts_bom']
    base_path = "data/erp_export"
    for dim in dimensions:
        os.makedirs(os.path.join(base_path, dim), exist_ok=True)
    print("Local staging directories created.")
    return base_path

def generate_erp_data():
    print("Generating Messy Enterprise Dimension Data...")
    
    # 1. Locations (Maintainence Hubs)
    locations = [
        {"location_id": "L-JFK", "airport": "JFK International", "city": "new york", "climate": "Temperate", "region": "North America"},
        {"location_id": "L-DXB", "airport": "Dubai International", "city": "DUBAI", "climate": "Desert/Hot", "region": "Middle East"},
        {"location_id": "L-LHR", "airport": "Heathrow", "city": "London", "climate": None, "region": "Europe"},
        {"location_id": "L-HND", "airport": "Haneda", "city": "Tokyo ", "climate": "Temperate/Humid", "region": "Asia"},
        {"location_id": "L-FRA", "airport": "Frankfurt", "city": "Frankfurt", "climate": "Temperate", "region": "Europe"}
    ]

    # 2. Aircraft Fleet
    airlines = ["Delta", "Emirates", "Lufthansa", "Singapore Airlines", "United"]
    aircraft = []
    tail_numbers = []
    for i in range(1, 51):  
        tail = f"N{1000 + i}GE"
        tail_numbers.append(tail)
        aircraft.append({
            "tail_number": tail,
            "airline_owner": random.choice(airlines),
            "fleet_status": random.choices(["Active", "In Maintenance"], weights=[0.9, 0.1])[0],
            "total_flight_hours": random.randint(5000, 45000)
        })
    aircraft.append(aircraft[0])

    # 3. Engines 
    models = [("GE90", "Boeing 777"), ("GEnx", "Boeing 787"), ("CFM56", "Airbus A320"), ("LEAP-1B", "Boeing 737 MAX")]
    engines = []
    base_date = datetime(2015, 1, 1)
    for engine_id in range(1, 101):
        model_info = random.choice(models)
        
        raw_date = base_date + timedelta(days=random.randint(0, 3000))
        if random.random() < 0.10:
            mfg_date = raw_date.strftime("%m/%d/%Y")
        else:
            mfg_date = raw_date.strftime("%Y-%m-%d")

        engines.append({
            "engine_id": engine_id,
            "tail_number": random.choice(tail_numbers), 
            "home_location_id": random.choice(locations)["location_id"], 
            "model": model_info[0],
            "aircraft_type_supported": model_info[1],
            "manufacture_date": mfg_date
        })

    # 4. Parts Bill of Materials (BOM)
    parts = []
    components = ["Titanium Fan Blade", "High Pressure Compressor", "Fuel Pump", "Combustion Chamber", "Low Pressure Turbine"]
    suppliers = ["AeroForge Inc.", "Global Turbine Parts", "Titanium Dynamics", "JetStream Suppliers"]
    for engine_id in range(1, 101):
        for comp in components:
            parts.append({
                "part_id": f"P-{engine_id}-{comp[:3].upper()}",
                "engine_id": engine_id,
                "component_name": comp,
                "supplier_name": random.choice(suppliers),
                "last_replaced_date": (datetime.now() - timedelta(days=random.randint(10, 1000))).strftime("%Y-%m-%d")
            })

    # 5. Thresholds
    thresholds = [
        {"sensor_name": "temp_hpc_out", "critical_limit": 1500.0, "alert_description": "High Temp Compressor Warning"},
        {"sensor_name": "physical_core_speed", "critical_limit": 9000.0, "alert_description": "Core Speed Redline"},
        {"sensor_name": "engine_pressure_ratio", "critical_limit": 1.5, "alert_description": "Pressure Drop Alert"}
    ]

    return {
        "dim_locations": locations,
        "dim_aircraft": aircraft,
        "dim_engines": engines,
        "dim_parts_bom": parts,
        "dim_thresholds": thresholds
    }

def save_to_local(base_path, datasets):
    for dim_name, data in datasets.items():
        file_path = os.path.join(base_path, dim_name, f"{dim_name}_export.json")
        with open(file_path, 'w') as f:
            for record in data:
                f.write(json.dumps(record) + '\n')
        print(f"Saved {len(data)} records to {file_path}")

if __name__ == "__main__":
    base_path = setup_local_folders()
    datasets = generate_erp_data()
    save_to_local(base_path, datasets)
    print("ERP Data Generation Complete!")