def list_versions(packages):
    from importlib.metadata import version, PackageNotFoundError
    
    result = {}
    for pkg in packages:
        try:
            result[pkg] = version(pkg)
        except PackageNotFoundError:
            result[pkg] = "Not installed"
    return result


if __name__ == "__main__":
    packages = [
        "python-dotenv",
        "schedule",
        "papermill",
        "pyarrow",
        "trino",
        "pymysql",
        "boto3",
        "unidecode",
        "pandas"
    ]

    versions = list_versions(packages)
    for k, v in versions.items():
        print(f"{k}=={v}")