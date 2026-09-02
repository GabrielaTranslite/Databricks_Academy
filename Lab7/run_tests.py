import os, sys, pytest

sys.dont_write_bytecode = True
script = globals().get("__file__") or sys.argv[0]
here = os.path.dirname(os.path.abspath(script))
tests_dir = os.path.join(here, "tests")

print("run_tests: here      =", here)
print("run_tests: tests_dir =", tests_dir, "exists =", os.path.isdir(tests_dir))
print("run_tests: what's in here =", os.listdir(here))

os.chdir(here)
sys.path.insert(0, here)
rc = pytest.main([tests_dir, "-p", "no:cacheprovider", "-v"])
if rc != 0:
    raise RuntimeError(f"pytest failed (exit code {rc})")