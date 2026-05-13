"""src/streaming/producer_case.py - Local producer example.

Reads sales from data/sales.csv
and writes records to a local simulated topic file one message at a time.

Start with main() at the bottom.
Work up to see how it all fits together.

Many functions are standard helpers
and should not need project-specific modifications.

Author: Denise Case
Date: 2026-05

Terminal command to run this file from the root project folder:

    uv run python -m streaming.producer_case

OBS:
  Don't edit this file - it should remain a working example.
  Copy it, rename it producer_yourname.py, and modify your copy.
"""

# === DECLARE IMPORTS ===

from collections.abc import Generator
import os
from pathlib import Path
import time
from typing import Any, Final

from datafun_streaming.io.errors import missing_csv_field_message
from datafun_streaming.io.io_utils import (
    append_csv_row,
    format_message_for_log,
    read_csv_rows,
)
from datafun_toolkit.logger import get_logger, log_header, log_path
from dotenv import load_dotenv

from streaming.core.utils import log_env_vars

# === CONFIGURE LOGGER ===

LOG = get_logger("P01", level="DEBUG")

# === LOAD ENVIRONMENT VARIABLES ===

load_dotenv(override=True)
log_env_vars(LOG)

# === DECLARE FALLBACK DEFAULTS ===

# WHY: These defaults intentionally do not match .env.example.
# If they appear in the log, copy .env.example to .env and try again.
DEFAULT_TOPIC_NAME: Final[str] = "streaming-01-topic-no-env"
DEFAULT_MESSAGE_COUNT: Final[str] = "2"
DEFAULT_MESSAGE_INTERVAL_SECONDS: Final[str] = "0.5"
DEFAULT_CLEAR_TOPIC_ON_START: Final[str] = "false"

# === DECLARE GLOBAL CONSTANTS ===

topic_name = os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC_NAME)
msg_count = os.getenv("PRODUCER_MESSAGE_COUNT", DEFAULT_MESSAGE_COUNT)
msg_interval_seconds = os.getenv(
    "PRODUCER_MESSAGE_INTERVAL_SECONDS",
    DEFAULT_MESSAGE_INTERVAL_SECONDS,
)
clear_topic_on_start = os.getenv(
    "KAFKA_CLEAR_TOPIC_ON_START",
    DEFAULT_CLEAR_TOPIC_ON_START,
)

TOPIC_NAME: Final[str] = topic_name
MESSAGE_COUNT: Final[int] = int(msg_count)
MESSAGE_INTERVAL_SECONDS: Final[float] = float(msg_interval_seconds)
CLEAR_TOPIC_ON_START: Final[bool] = clear_topic_on_start.strip().lower() == "true"

# === DECLARE CONSTANT PATHS ===

ROOT_DIR: Final[Path] = Path.cwd()
DATA_DIR: Final[Path] = ROOT_DIR / "data"
OUTPUT_DIR: Final[Path] = DATA_DIR / "output"

SALES_CSV: Final[Path] = DATA_DIR / "sales.csv"
TOPIC_CSV: Final[Path] = OUTPUT_DIR / f"{TOPIC_NAME}.csv"


# ==========================================================
# DEFINE SECTION A. ACQUIRE RESOURCES AND GET READY HELPERS
# ==========================================================


def log_paths() -> None:
    """Log run header and all paths."""
    log_header(LOG, "P01")
    LOG.info("========================")
    LOG.info("START producer main()")
    LOG.info("========================")
    log_path(LOG, "ROOT_DIR", ROOT_DIR)
    log_path(LOG, "DATA_DIR", DATA_DIR)
    log_path(LOG, "SALES_CSV", SALES_CSV)
    log_path(LOG, "TOPIC_CSV", TOPIC_CSV)


def load_settings() -> None:
    """Load local producer settings from .env and log them."""
    LOG.info("Loading settings from .env...")
    LOG.info(f"KAFKA_TOPIC                       = {TOPIC_NAME}")
    LOG.info(f"KAFKA_CLEAR_TOPIC_ON_START        = {CLEAR_TOPIC_ON_START}")
    LOG.info(f"PRODUCER_MESSAGE_COUNT            = {MESSAGE_COUNT}")
    LOG.info(f"PRODUCER_MESSAGE_INTERVAL_SECONDS = {MESSAGE_INTERVAL_SECONDS}")


def verify_source() -> None:
    """Verify the local source file exists.

    Raises:
        SystemExit: If the source file does not exist.
    """
    LOG.info("Verifying local source data...")

    if not SALES_CSV.exists():
        LOG.error(f"Source file not found: {SALES_CSV}")
        raise SystemExit(1)

    LOG.info(f"Source file found: {SALES_CSV.name}")


def prepare_topic_file() -> None:
    """Prepare the local simulated topic file.

    If KAFKA_CLEAR_TOPIC_ON_START is true, delete the existing topic file
    so the producer starts with a clean local topic.

    If KAFKA_CLEAR_TOPIC_ON_START is false, keep existing messages and append.
    """
    LOG.info("Preparing local simulated topic file...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if TOPIC_CSV.exists() and CLEAR_TOPIC_ON_START:
        TOPIC_CSV.unlink()
        LOG.info(f"Deleted existing topic file: {TOPIC_CSV.name}")

    if TOPIC_CSV.exists():
        LOG.info(f"Using existing topic file: {TOPIC_CSV.name}")
    else:
        LOG.info(f"Topic file will be created: {TOPIC_CSV.name}")


# ===========================================================================
# DEFINE SECTION P. PRODUCE MESSAGES HELPERS
# ===========================================================================


def get_message_key(message: dict[str, Any]) -> str:
    """Return the message key for a sale record.

    Module 01 does not use Kafka yet, but this function prepares the same
    shape used later when the key is sent to Kafka.
    """
    try:
        return str(message["region_id"])
    except KeyError as error:
        msg = missing_csv_field_message(
            field="region_id",
            available_fields=list(message.keys()),
        )
        raise KeyError(msg) from error


def generate_messages(count: int) -> Generator[dict[str, str]]:
    """Generate a stream of sales from the input CSV file.

    Arguments:
        count: How many sales to generate.

    Yields:
        One sale row dictionary at a time.
    """
    # Call the function read_csv_rows() to read
    # all rows from the SALES_CSV file into a list of dictionaries.
    # A dictionary is a set of key-value pairs,
    # where the keys are the column names from the CSV file
    # Note keys and values are all strings when read from CSV.
    # To use values, we will need to convert them from strings to the appropriate types.
    sales_rows: list[dict[str, str]] = read_csv_rows(SALES_CSV)

    # Use a generator to yield one sale row at a time, up to the specified count.
    # In Python, [start:stop] slicing syntax is used to get a subset of a list.
    # Since we don't provide a start index, it defaults to 0 (the beginning of the list).
    # So this will start at the first row and yield up to 'count' rows.
    yield from sales_rows[:count]


def send_local_message(message: dict[str, Any]) -> None:
    """Write one message to the local simulated topic file."""
    append_csv_row(
        path=TOPIC_CSV,
        row=message,
        fieldnames=list(message.keys()),
    )


def send_messages() -> int:
    """Generate and write local messages one at a time."""
    LOG.info("Sending messages...")
    LOG.info(f"Sending up to {MESSAGE_COUNT} local message(s).")
    LOG.info(f"Writing to simulated topic file: {TOPIC_CSV.name}")
    LOG.info("Watch each sale arrive. Press CTRL+C to stop early.\n")

    sent_count: int = 0

    try:
        for message in generate_messages(MESSAGE_COUNT):
            LOG.info(format_message_for_log(message))

            key = get_message_key(message)
            LOG.info(f"  Sending local message with key={key}")

            send_local_message(message)

            sent_count += 1
            LOG.info(f"  MESSAGE SENT  sent={sent_count}")
            time.sleep(MESSAGE_INTERVAL_SECONDS)

    except (FileNotFoundError, KeyError, RuntimeError, ValueError) as error:
        LOG.error(str(error))
        LOG.error("Producer stopped before completing all messages.")
        raise SystemExit(1) from error

    return sent_count


# ===========================================================================
# DEFINE SECTION E. EXIT AND CLEANUP HELPERS
# ===========================================================================


def log_summary(sent_count: int) -> None:
    """Log final summary statistics."""
    LOG.info("Summary:")
    LOG.info(f"Sent {sent_count} message(s).")
    log_path(LOG, "WROTE TOPIC_CSV", TOPIC_CSV)
    LOG.info("========================")
    LOG.info("Producer executed successfully!")
    LOG.info("========================")


# ===========================================================================
# MAIN FUNCTION
# ===========================================================================


def main() -> None:
    """Main entry point for the local producer."""
    log_paths()

    LOG.info("========================")
    LOG.info("SECTION A. Acquire")
    LOG.info("========================")

    load_settings()
    verify_source()
    prepare_topic_file()

    LOG.info("========================")
    LOG.info("SECTION P. Produce Messages")
    LOG.info("========================")

    sent_count: int = send_messages()

    LOG.info("========================")
    LOG.info("SECTION E. Exit")
    LOG.info("========================")

    log_summary(sent_count)


# === CONDITIONAL EXECUTION GUARD ===

if __name__ == "__main__":
    main()
