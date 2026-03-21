from storage_manager import StorageManager

def verify():
    print("Verifying Assignment Setup...")
    print("-" * 50)
    
    try:
        from ingestion import stream_records
        from normalize import normalize_record
        from analyzer import Analyzer
        from classifier import classify
        print("✓ All modules imported successfully")
    except Exception as e:
        print(f"✗ Module import failed: {e}")
        return False
    
    storage = StorageManager()
    if not storage.connect():
        print("✗ Database connection failed")
        print("\nAction needed:")
        print("  MySQL:   net start MySQL80 (Run as Admin)")
        print("  MongoDB: /c/MongoDB/bin/mongod.exe --dbpath=/c/data/db --logpath=/c/data/log/mongod.log")
        storage.close()
        return False
    
    storage.close()
    
    print("\n" + "=" * 50)
    print("✓✓ ALL CHECKS PASSED - Ready to run main.py")
    print("=" * 50)
    return True

if __name__ == "__main__":
    verify()
