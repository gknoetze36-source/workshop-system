from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from models.core import Base

class Special(Base):
    __tablename__ = "flyer_lady_specials"
    __table_args__ = (Index("ix_flyer_specials_location_status", "location_id", "status"), Index("ix_flyer_specials_location_created", "location_id", "created_at"))
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    created_by: Mapped[str] = mapped_column(String(100), nullable=False)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    media_url: Mapped[Optional[str]] = mapped_column(String(2000))
    booking_link: Mapped[str] = mapped_column(String(2000), nullable=False)
    status: Mapped[str] = mapped_column(String(30), default="draft", nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

class SpecialApproval(Base):
    __tablename__ = "flyer_lady_special_approvals"
    __table_args__ = (Index("ix_flyer_approvals_location_special", "location_id", "special_id", "decided_at"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    special_id: Mapped[int] = mapped_column(ForeignKey("flyer_lady_specials.id", ondelete="CASCADE"), nullable=False)
    decision: Mapped[str] = mapped_column(String(20), nullable=False)
    decided_by: Mapped[str] = mapped_column(String(100), nullable=False)
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

class SpecialPost(Base):
    __tablename__ = "flyer_lady_special_posts"
    __table_args__ = (UniqueConstraint("special_id", "platform", name="uq_flyer_special_platform"), Index("ix_flyer_posts_location_status", "location_id", "status"), Index("ix_flyer_posts_special", "special_id"))
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    special_id: Mapped[int] = mapped_column(ForeignKey("flyer_lady_specials.id", ondelete="CASCADE"), nullable=False)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    platform: Mapped[str] = mapped_column(String(40), nullable=False)
    external_post_id: Mapped[Optional[str]] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(30), default="pending", nullable=False)
    published_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    next_attempt_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

class FlyerLinkClick(Base):
    __tablename__ = "flyer_lady_link_clicks"
    __table_args__ = (Index("ix_flyer_clicks_special_created", "special_id", "created_at"), Index("ix_flyer_clicks_location_created", "location_id", "created_at"))
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    special_id: Mapped[int] = mapped_column(ForeignKey("flyer_lady_specials.id", ondelete="CASCADE"), nullable=False)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    user_agent: Mapped[Optional[str]] = mapped_column(String(1000))
    referrer: Mapped[Optional[str]] = mapped_column(String(2000))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)


class FlyerPublicLink(Base):
    """Minimal public lookup record; contains no special copy or credentials."""
    __tablename__ = "flyer_lady_public_links"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    special_id: Mapped[int] = mapped_column(ForeignKey("flyer_lady_specials.id", ondelete="CASCADE"), unique=True, nullable=False)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    target_url: Mapped[str] = mapped_column(String(2000), nullable=False)
    active: Mapped[bool] = mapped_column(default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
