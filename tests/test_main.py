def test_import_src_main():
    # This test verifies that src.main imports successfully and that the
    # application module loads without crashing because of syntax errors,
    # broken imports, or top-level initialization issues.
    #
    # noqa: F401 suppresses the unused-import warning because the import
    # itself is what this test is validating.
    import src.main  # noqa: F401
