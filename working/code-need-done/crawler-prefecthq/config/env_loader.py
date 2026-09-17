import logging
import os

logger = logging.getLogger(__name__)


def load_env_file(env_file=".env"):
    """
    Load simple KEY=VALUE pairs from a .env file.

    No external dependency required.
    Existing environment variables are NOT overwritten.
    """

    if not os.path.exists(env_file):
        logger.warning(
            ".env file not found: %s",
            os.path.abspath(env_file)
        )
        return

    loaded = 0

    with open(env_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()

            # Empty line
            if not line:
                continue

            # Comment
            if line.startswith("#"):
                continue

            # Invalid line
            if "=" not in line:
                continue

            key, value = line.split("=", 1)

            key = key.strip()
            value = value.strip()

            if not key:
                continue

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

            # Do not overwrite existing OS environment
            if key not in os.environ:
                os.environ[key] = value
                loaded += 1

    logger.info(
        "Loaded %d environment variables from %s",
        loaded,
        os.path.abspath(env_file)
    )