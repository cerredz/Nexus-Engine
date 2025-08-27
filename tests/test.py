import importlib
import os
import sys
import traceback

# Ensure project root is on sys.path so tests can import the `engine` package
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

def run_all_tests():
    test_dir = os.path.dirname(__file__)
    test_files = [f for f in os.listdir(test_dir) if f.startswith("test_") and f.endswith(".py") and f != "test.py"]

    total = len(test_files)
    passed = 0
    failed = 0
    skipped = 0

    for file in test_files:
        module_name = file[:-3]
        try:
            module = importlib.import_module(module_name)
            print(f"Running {module_name}...")
            test_funcs = [getattr(module, name) for name in dir(module) if name.startswith("test") and callable(getattr(module, name))]

            if not test_funcs:
                print(f"{module_name} SKIPPED (no test functions found)")
                skipped += 1
                continue

            for func in test_funcs:
                func()

            print(f"{module_name} PASSED")
            passed += 1
        except Exception:
            print(f"{module_name} FAILED")
            traceback.print_exc()
            failed += 1

    print(f"\nSummary: {passed} passed, {failed} failed, {skipped} skipped out of {total}")

if __name__ == "__main__":
    run_all_tests()