from datetime import datetime, timedelta


def date_range_generator(day_from: str, day_to: str):
    start_date = datetime.strptime(
        day_from,
        "%Y-%m-%d"
    ).date()

    end_date = datetime.strptime(
        day_to,
        "%Y-%m-%d"
    ).date()

    if start_date > end_date:
        raise ValueError(
            f"day_from ({day_from}) > day_to ({day_to})"
        )

    current_date = start_date

    while current_date <= end_date:
        yield current_date.strftime("%Y-%m-%d")
        current_date += timedelta(days=1)