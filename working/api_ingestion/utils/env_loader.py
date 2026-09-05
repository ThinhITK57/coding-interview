import os


def load_env(path=".env"):
    """
    Load environment variables from a .env file.

    Existing environment variables are NOT overwritten.
    """

    if not os.path.exists(path):
        return

    with open(path, "r", encoding="utf-8") as file:

        for raw_line in file:

            line = raw_line.strip()

            # Empty line
            if not line:
                continue

            # Comment
            if line.startswith("#"):
                continue

            if "=" not in line:
                continue

            key, value = line.split("=", 1)

            key = key.strip()
            value = value.strip()

            # Remove surrounding quotes
            if len(value) >= 2:

                if (
                    value.startswith('"')
                    and value.endswith('"')
                ):
                    value = value[1:-1]

                elif (
                    value.startswith("'")
                    and value.endswith("'")
                ):
                    value = value[1:-1]

            # Don't overwrite real environment variables
            os.environ.setdefault(key, value)