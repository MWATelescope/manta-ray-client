from datetime import datetime


def format_expiry(timestamp_str: str) -> str:
    """Format timestamp showing time until expiry"""

    try:
        expiry = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        now = datetime.now(expiry.tzinfo)
        delta = expiry - now

        if delta.total_seconds() < 0:
            return f"{timestamp_str} [red](expired)[/red]"

        hours = delta.total_seconds() / 3600
        if hours < 1:
            minutes = int(delta.total_seconds() / 60)
            return f"{timestamp_str} [green](expires in {minutes} minutes)[/green]"
        elif hours < 24:
            return f"{timestamp_str} [green](expires in {int(hours)} hours)[/green]"
        else:
            days = int(delta.total_seconds() / 86400)
            return f"{timestamp_str} [green](expires in {days} days)[/green]"
    except Exception:
        return timestamp_str
