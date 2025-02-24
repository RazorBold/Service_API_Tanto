import pymysql
from pprint import pprint
import requests
import time
from datetime import datetime

# Database Configuration for local access
DB_HOST = '127.0.0.1'
DB_PORT = 3306
DB_USER = 'admin'
DB_PASSWORD = 'Wow0w0!2025'
DB_NAME = 'lansitec_cat1'

def clean_serial_number(serial_number):
    """Remove spaces and hyphens from serial number"""
    return serial_number.replace(' ', '').replace('-', '')

def is_valid_device(serial_number):
    valid_prefixes = ('TAKU', 'CICU', 'ICBU')
    return any(serial_number.startswith(prefix) for prefix in valid_prefixes)

def get_tracking_data(serial_number):
    """Fetch tracking data from API"""
    url = f"https://sync.tantoline.id/edoc/service/tracking?c={serial_number}"
    try:
        response = requests.get(url)
        return response.json()
    except Exception as e:
        return {"error": str(e)}

def create_service_table(connection):
    """Create service_tanto table if it doesn't exist"""
    with connection.cursor() as cursor:
        sql = """
        CREATE TABLE IF NOT EXISTS service_tanto (
            id INT AUTO_INCREMENT PRIMARY KEY,
            id_container VARCHAR(50),
            last_activity TEXT,
            date VARCHAR(50),
            created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        cursor.execute(sql)
        connection.commit()

def get_latest_date(cursor, container_number):
    """Check if date exists for container"""
    sql = "SELECT date FROM service_tanto WHERE id_container = %s ORDER BY id DESC LIMIT 1"
    cursor.execute(sql, (container_number,))
    result = cursor.fetchone()
    return result['date'] if result else None

def save_tracking_data(connection, tracking_data):
    """Save tracking data if it's new"""
    if not tracking_data.get('data'):
        return False
    
    data = tracking_data['data'][0]
    with connection.cursor() as cursor:
        # Check if this date already exists for this container
        latest_date = get_latest_date(cursor, data['container_number'])
        if latest_date == data['date']:
            return False
            
        # Insert new record
        sql = """
        INSERT INTO service_tanto (id_container, last_activity, date)
        VALUES (%s, %s, %s)
        """
        cursor.execute(sql, (
            data['container_number'],
            data['last_activity'],
            data['date']
        ))
        connection.commit()
        return True

def get_db_connection():
    """Create database connection"""
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor
    )

def process_devices():
    """Main processing function"""
    try:
        connection = get_db_connection()
        print("✅ Database connected")
        
        # Create table if not exists
        create_service_table(connection)
        print("✅ Table verified/created")
        
        while True:
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT * FROM device")
                    devices = [d for d in cursor.fetchall() if is_valid_device(d['serial_number'])]
                
                for device in devices:
                    cleaned_serial = clean_serial_number(device['serial_number'])
                    tracking_data = get_tracking_data(cleaned_serial)
                    
                    if tracking_data.get('status'):
                        if save_tracking_data(connection, tracking_data):
                            print(f"✅ New data saved for {cleaned_serial}")
                        else:
                            print(f"ℹ️ No new data for {cleaned_serial}")
                    else:
                        print(f"❌ Error fetching data for {cleaned_serial}")
                
                print("\n💤 Waiting 10 seconds...\n")
                time.sleep(10)
                
            except Exception as e:
                print(f"❌ Error in loop: {str(e)}")
                time.sleep(10)  # Wait before retrying
                
    except Exception as e:
        print(f"❌ Error: {str(e)}")
    finally:
        if 'connection' in locals():
            connection.close()

if __name__ == "__main__":
    print("🚀 Starting continuous tracking service...")
    process_devices()
