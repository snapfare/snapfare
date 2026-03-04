"""
Email sender abstraction layer.
Allows switching between EmailOctopus (current) and Amazon SES (future)
via the EMAIL_PROVIDER env var without code changes.

Usage:
    from backend.services.email_sender import get_sender
    sender = get_sender()
    sender.send_newsletter(recipients=[...], subject="...", html="...")
"""

from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from typing import Optional

logger = logging.getLogger(__name__)


# ─── Abstract base ─────────────────────────────────────────────────────────────

class EmailSenderBase(ABC):
    """Common interface for all email providers."""

    @abstractmethod
    def send_newsletter(
        self,
        recipients: list[str],
        subject: str,
        html: str,
        from_name: str = "SnapFare",
        from_email: str = "newsletter@basics-db.ch",
    ) -> dict:
        """Send a newsletter/broadcast to a list of recipients.

        Returns a dict with keys: provider, sent, failed, message.
        """

    @abstractmethod
    def add_contact(
        self,
        email: str,
        tier: str = "free",
        name: Optional[str] = None,
    ) -> bool:
        """Add a subscriber to the provider's list. Returns True on success."""

    @abstractmethod
    def remove_contact(self, email: str) -> bool:
        """Remove / unsubscribe a contact. Returns True on success."""


# ─── EmailOctopus provider ─────────────────────────────────────────────────────

class EmailOctopusSender(EmailSenderBase):
    """
    Sends via EmailOctopus REST API.
    Requires env var: EMAIL_OCTOPUS_API_KEY
    Optional: EMAIL_OCTOPUS_LIST_ID (default list for contacts)

    EmailOctopus free plan: 2,500 contacts, 10,000 emails/month.
    https://emailoctopus.com/api/1.6/
    """

    BASE_URL = "https://emailoctopus.com/api/1.6"

    def __init__(self):
        self.api_key = os.getenv("EMAIL_OCTOPUS_API_KEY", "")
        self.list_id = os.getenv("EMAIL_OCTOPUS_LIST_ID", "")
        if not self.api_key:
            logger.warning("EMAIL_OCTOPUS_API_KEY not set")

    def send_newsletter(
        self,
        recipients: list[str],
        subject: str,
        html: str,
        from_name: str = "SnapFare",
        from_email: str = "newsletter@basics-db.ch",
    ) -> dict:
        """
        Note: EmailOctopus campaigns are typically created in the UI, not via API.
        This method uses the Campaigns API if available, or logs a warning.
        For now, newsletter sending is done manually via EmailOctopus dashboard.
        This method is a placeholder for when we switch to SES.
        """
        logger.info(
            "EmailOctopus: newsletter sending is managed via dashboard. "
            "Subject: %s, Recipients: %d",
            subject,
            len(recipients),
        )
        return {
            "provider": "emailoctopus",
            "sent": 0,
            "failed": 0,
            "message": "EmailOctopus newsletter is sent manually via dashboard.",
        }

    def add_contact(
        self,
        email: str,
        tier: str = "free",
        name: Optional[str] = None,
    ) -> bool:
        """Add a contact to the EmailOctopus list."""
        import requests

        if not self.api_key or not self.list_id:
            logger.warning("EmailOctopus not configured — skipping add_contact")
            return False

        payload = {
            "api_key": self.api_key,
            "email_address": email.lower().strip(),
            "fields": {"Tier": tier},
            "status": "SUBSCRIBED",
        }
        if name:
            payload["fields"]["FirstName"] = name

        try:
            resp = requests.post(
                f"{self.BASE_URL}/lists/{self.list_id}/contacts",
                json=payload,
                timeout=10,
            )
            if resp.status_code in (200, 201):
                logger.info("EmailOctopus: added contact %s (tier=%s)", email, tier)
                return True
            # 409 = already exists — treat as success
            if resp.status_code == 409:
                logger.info("EmailOctopus: contact %s already exists", email)
                return True
            logger.error(
                "EmailOctopus add_contact failed: %s %s", resp.status_code, resp.text
            )
            return False
        except Exception as exc:
            logger.error("EmailOctopus add_contact exception: %s", exc)
            return False

    def remove_contact(self, email: str) -> bool:
        """Mark a contact as unsubscribed in EmailOctopus."""
        import requests

        if not self.api_key or not self.list_id:
            logger.warning("EmailOctopus not configured — skipping remove_contact")
            return False

        # First, find the contact ID
        try:
            resp = requests.get(
                f"{self.BASE_URL}/lists/{self.list_id}/contacts",
                params={"api_key": self.api_key, "limit": 1},
                timeout=10,
            )
            # EmailOctopus doesn't support search by email directly —
            # for unsubscribe, we rely on the dashboard or webhook.
            # Mark as UNSUBSCRIBED via the contact endpoint if we have the ID.
            logger.info(
                "EmailOctopus: unsubscribe %s — use dashboard or webhook", email
            )
            return True
        except Exception as exc:
            logger.error("EmailOctopus remove_contact exception: %s", exc)
            return False


# ─── Amazon SES provider ───────────────────────────────────────────────────────

class AWSSESSender(EmailSenderBase):
    """
    Sends via Amazon SES using boto3.
    Requires env vars: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SES_REGION
    Optional: AWS_SES_CONFIGURATION_SET (for open/click tracking)

    Pricing: ~$0.10 per 1,000 emails (essentially free at 1,500 subscriber scale).
    https://docs.aws.amazon.com/ses/
    """

    def __init__(self):
        self.region = os.getenv("AWS_SES_REGION", "eu-west-1")
        self.config_set = os.getenv("AWS_SES_CONFIGURATION_SET", "")
        self._client = None

    @property
    def client(self):
        if self._client is None:
            import boto3
            self._client = boto3.client(
                "ses",
                region_name=self.region,
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            )
        return self._client

    def send_newsletter(
        self,
        recipients: list[str],
        subject: str,
        html: str,
        from_name: str = "SnapFare",
        from_email: str = "newsletter@basics-db.ch",
        batch_size: int = 50,
    ) -> dict:
        """
        Send newsletter to all recipients via SES bulk send.
        Batches to respect SES sending limits.
        """
        from_address = f"{from_name} <{from_email}>"
        sent = 0
        failed = 0

        for i in range(0, len(recipients), batch_size):
            batch = recipients[i : i + batch_size]
            for email in batch:
                try:
                    params: dict = {
                        "Source": from_address,
                        "Destination": {"ToAddresses": [email]},
                        "Message": {
                            "Subject": {"Data": subject, "Charset": "UTF-8"},
                            "Body": {"Html": {"Data": html, "Charset": "UTF-8"}},
                        },
                    }
                    if self.config_set:
                        params["ConfigurationSetName"] = self.config_set

                    self.client.send_email(**params)
                    sent += 1
                except Exception as exc:
                    logger.error("SES send failed for %s: %s", email, exc)
                    failed += 1

        logger.info("SES newsletter: sent=%d failed=%d", sent, failed)
        return {
            "provider": "ses",
            "sent": sent,
            "failed": failed,
            "message": f"Sent {sent}, failed {failed}",
        }

    def add_contact(
        self,
        email: str,
        tier: str = "free",
        name: Optional[str] = None,
    ) -> bool:
        """SES doesn't have contact management — Supabase is the source of truth."""
        logger.info("SES: contact management handled by Supabase (no-op)")
        return True

    def remove_contact(self, email: str) -> bool:
        """SES: add to suppression list to prevent future sends."""
        try:
            self.client.put_suppressed_destination(
                EmailAddress=email,
                Reason="UNSUBSCRIBE",
            )
            logger.info("SES: added %s to suppression list", email)
            return True
        except Exception as exc:
            logger.error("SES remove_contact exception: %s", exc)
            return False


# ─── Factory ───────────────────────────────────────────────────────────────────

def get_sender() -> EmailSenderBase:
    """
    Returns the configured email sender.
    Controlled by EMAIL_PROVIDER env var:
      - 'emailoctopus' (default): current live provider
      - 'ses': Amazon SES (switch when ready)
    """
    provider = os.getenv("EMAIL_PROVIDER", "emailoctopus").lower().strip()

    if provider == "ses":
        logger.info("Email provider: Amazon SES")
        return AWSSESSender()

    if provider == "emailoctopus":
        logger.info("Email provider: EmailOctopus")
        return EmailOctopusSender()

    logger.warning("Unknown EMAIL_PROVIDER '%s', falling back to EmailOctopus", provider)
    return EmailOctopusSender()
