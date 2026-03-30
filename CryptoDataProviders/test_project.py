"""
Quick test script - verify that the CryptoDataProviders project is working properly
"""
import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_imports():
    """Test whether all modules can be imported normally"""
    print("=" * 50)
    print("Test 1: Check module imports")
    print("=" * 50)
    
    try:
        # Test binance_api
        from providers.binance_api.market_api import fetch_klines
        print("✓ providers.binance_api Import successful")
        
        # Test binance_bulk
        from providers.binance_bulk.bulk_fetcher import BinanceBulkFetcher
        print("✓ providers.binance_bulk Import successful")
        
        # test utils
        from utils.common import parse_time, ProgressTracker
        print("✓ utils.common Import successful")
        
        from utils.trading_pairs import get_trading_pairs
        print("✓ utils.trading_pairs Import successful")
        
        # Test the main module
        import config
        print("✓ config Import successful")
        
        print("\nAll module import tests passed!")
        return True
        
    except Exception as e:
        print(f"\n✗ Module import failed:{e}")
        import traceback
        traceback.print_exc()
        return False


def test_dependencies():
    """Test whether dependent packages are installed"""
    print("\n" + "=" * 50)
    print("Test 2: Check dependencies")
    print("=" * 50)
    
    dependencies = [
        "pandas",
        "pyarrow",
        "requests",
        "tqdm",
        "rich",
        "numpy"
    ]
    
    all_installed = True
    
    for dep in dependencies:
        try:
            __import__(dep)
            print(f"✓ {dep} Installed")
        except ImportError:
            print(f"✗ {dep} Not installed")
            all_installed = False
    
    if all_installed:
        print("\nAll dependent packages have been installed!")
    else:
        print("\nSome dependent packages are not installed, please run: pip install -r requirements.txt")
    
    return all_installed


def test_basic_functionality():
    """Test basic functionality"""
    print("\n" + "=" * 50)
    print("Test 3: Test basic functionality")
    print("=" * 50)
    
    try:
        from utils.common import parse_time
        from datetime import datetime, timezone
        
        # Test time analysis
        dt = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        timestamp = parse_time(dt)
        print(f"✓ Time parsing test passed:{dt} -> {timestamp}")
        
        # Test string time parsing
        timestamp2 = parse_time("2025-01-01 00:00:00")
        print(f"✓ String time parsing test passed:'2025-01-01 00:00:00' -> {timestamp2}")
        
        print("\nBasic function test passed!")
        return True
        
    except Exception as e:
        print(f"\n✗ Basic functional test failed:{e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests"""
    print("\n" + "=" * 60)
    print("CryptoDataProviders Project testing")
    print("=" * 60 + "\n")
    
    results = []
    
    # Run all tests
    results.append(("module import", test_imports()))
    results.append(("Dependency package check", test_dependencies()))
    results.append(("Basic functions", test_basic_functionality()))
    
    # Output test results
    print("\n" + "=" * 60)
    print("Summary of test results")
    print("=" * 60)
    
    for test_name, passed in results:
        status = "✓ pass" if passed else "✗ fail"
        print(f"{test_name:20s}: {status}")
    
    all_passed = all(passed for _, passed in results)
    
    if all_passed:
        print("\n" + "=" * 60)
        print("🎉 All tests passed! The project can be used normally.")
        print("=" * 60)
        print("\nNext step:")
        print("1. Check out README.md for detailed usage instructions")
        print("2. Run the example scripts in the examples/ directory")
        print("3. Start using CryptoDataProviders to get data")
    else:
        print("\n" + "=" * 60)
        print("⚠️ Some tests failed, please check the above error message.")
        print("=" * 60)
        print("\nFAQ:")
        print("1. If the dependent package is not installed: pip install -r requirements.txt")
        print("2. If there are network problems: Configure the proxy in config.py")
        print("3. If there are other issues: View error stack information")
    
    return all_passed


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
