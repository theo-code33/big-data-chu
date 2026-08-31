from __future__ import annotations

import hashlib

from src.config import EDS_PSEUDO_SALT


def patient_pseudo(patient_id: str) -> str:
    """Hash déterministe (sel + IPP) : jointures stables, non réversible sans le sel."""
    payload = f"{EDS_PSEUDO_SALT}|{patient_id}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
