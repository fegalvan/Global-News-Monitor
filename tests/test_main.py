def test_import_src_main():
    import src.main  # noqa: F401
## check if src/main.py loads without crashing, fail if errors, bad imports, etc
## noqa: F401 prevents error caused by importing and being unused